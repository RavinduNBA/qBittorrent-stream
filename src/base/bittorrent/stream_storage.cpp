#include "stream_storage.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <iostream>

#include <boost/asio/error.hpp>

namespace BitTorrent
{
    SlidingWindowStorage::SlidingWindowStorage(const lt::file_storage &fs, const StreamOptions &opts)
        : m_files(fs), m_opts(opts)
    {
        m_totalPieces = m_files.is_valid() ? m_files.num_pieces() : 0;

        if (!m_opts.fifoPath.empty())
        {
            m_fifoFd = ::open(m_opts.fifoPath.c_str(), O_RDWR | O_NONBLOCK | O_CREAT, 0666);
        }

        if (!m_opts.outputPath.empty())
        {
            m_outputFd = ::open(m_opts.outputPath.c_str(), O_RDWR | O_CREAT, 0666);
            if (m_outputFd >= 0)
            {
                m_opts.closeFdOnExit = true;
            }
            else
            {
                m_outputFd = m_opts.outputFd;
            }
        }
        else
        {
            m_outputFd = m_opts.outputFd;
        }

        FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
        if (dbg) {
            std::fprintf(dbg, "[StreamStorage] Init targetDirPath='%s', fifoPath='%s', num_files=%d, total_size=%lld, totalPieces=%d\n",
                         m_opts.targetDirPath.c_str(), m_opts.fifoPath.c_str(), m_files.num_files(),
                         static_cast<long long>(m_files.total_size()), m_totalPieces);
            std::fclose(dbg);
        }
    }

    SlidingWindowStorage::~SlidingWindowStorage()
    {
        tryFlushAndEvict();
        if (m_currentFileFd >= 0)
        {
            ::close(m_currentFileFd);
            m_currentFileFd = -1;
        }
        if (m_fifoFd >= 0)
        {
            ::close(m_fifoFd);
            m_fifoFd = -1;
        }
        if (m_opts.closeFdOnExit && (m_outputFd >= 0))
        {
            ::close(m_outputFd);
            m_outputFd = -1;
        }
    }

    int SlidingWindowStorage::calculatePieceSize(lt::piece_index_t piece) const
    {
        if (!m_files.is_valid())
            return 0;
        return m_files.piece_size(piece);
    }

    std::size_t SlidingWindowStorage::activeBufferedBytes() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::size_t total = 0;
        for (const auto &kv : m_window)
            total += kv.second.data.size();
        return total;
    }

    bool SlidingWindowStorage::isWriteQueueFull() const
    {
        return activeBufferedBytes() >= m_opts.maxBufferBytes;
    }

    lt::span<const char> SlidingWindowStorage::readv(const lt::peer_request &r, lt::storage_error &ec)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_window.find(r.piece);
        if (it == m_window.end())
        {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::operation_aborted;
            return {};
        }

        const auto &slot = it->second;
        if (static_cast<int>(slot.data.size()) <= r.start)
        {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::operation_aborted;
            return {};
        }

        const int available = static_cast<int>(slot.data.size()) - r.start;
        return {slot.data.data() + r.start, static_cast<std::ptrdiff_t>(std::min(r.length, available))};
    }

    void SlidingWindowStorage::writev(lt::span<const char> b, lt::piece_index_t piece, int offset)
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        // Strict memory bound: ONLY accept blocks within active sliding window [m_headPiece, m_headPiece + 32)
        if (piece < m_headPiece || static_cast<int>(piece) >= static_cast<int>(m_headPiece) + 32)
            return;

        // Blocks are accepted into memory; isWriteQueueFull handles backpressure

        auto &slot = m_window[piece];
        if (slot.data.empty())
        {
            slot.size = calculatePieceSize(piece);
            slot.data.resize(static_cast<std::size_t>(slot.size), 0);
            slot.totalBlocks = (slot.size + lt::default_block_size - 1) / lt::default_block_size;
            slot.blocksReceived.assign(static_cast<std::size_t>(slot.totalBlocks), false);
        }

        if (offset + b.size() <= static_cast<std::ptrdiff_t>(slot.data.size()))
        {
            std::memcpy(slot.data.data() + offset, b.data(), static_cast<std::size_t>(b.size()));
            const int blockIdx = offset / lt::default_block_size;
            if ((blockIdx >= 0) && (blockIdx < slot.totalBlocks))
            {
                if (!slot.blocksReceived[static_cast<std::size_t>(blockIdx)])
                {
                    slot.blocksReceived[static_cast<std::size_t>(blockIdx)] = true;
                    slot.blocksDone++;
                    if ((slot.blocksDone % 16 == 0) || (slot.blocksDone == slot.totalBlocks))
                    {
                        FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
                        if (dbg) {
                            std::fprintf(dbg, "[StreamStorage] writev: piece=%d blocks=%d/%d\n",
                                static_cast<int>(piece), slot.blocksDone, slot.totalBlocks);
                            std::fclose(dbg);
                        }
                    }
                }
            }
        }
    }

    void SlidingWindowStorage::clearPiece(lt::piece_index_t piece)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_window.erase(piece);
    }

    lt::sha1_hash SlidingWindowStorage::hash(lt::piece_index_t piece,
                                             lt::span<lt::sha256_hash> blockHashes,
                                             lt::storage_error &ec)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        const auto it = m_window.find(piece);
        if ((it == m_window.end()) || it->second.data.empty())
        {
            // Clear error so libtorrent treats this as a piece hash failure (re-download)
            // rather than a fatal storage hardware failure (which would pause the torrent).
            ec = lt::storage_error();
            return {};
        }

        auto &slot = it->second;

        if (!blockHashes.empty())
        {
            const int blocksInPiece = (slot.size + lt::default_block_size - 1) / lt::default_block_size;
            const char *buf = slot.data.data();
            for (int k = 0; (k < blocksInPiece) && (k < static_cast<int>(blockHashes.size())); ++k)
            {
                const std::ptrdiff_t len = std::min(lt::default_block_size, slot.size - (k * lt::default_block_size));
                blockHashes[k] = lt::hasher256(lt::span<const char>{buf + (k * lt::default_block_size), len}).final();
            }
        }

        const lt::sha1_hash result = lt::hasher(slot.data).final();
        slot.verified = true;

        FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
        if (dbg) {
            std::fprintf(dbg, "[StreamStorage] hash: piece=%d verified, size=%zu\n", static_cast<int>(piece), slot.data.size());
            std::fclose(dbg);
        }

        lock.unlock();
        tryFlushAndEvict();

        return result;
    }

    lt::sha256_hash SlidingWindowStorage::hash2(lt::piece_index_t piece, int offset, lt::storage_error &ec)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_window.find(piece);
        if ((it == m_window.end()) || it->second.data.empty())
        {
            ec = lt::storage_error();
            return {};
        }

        const auto &slot = it->second;
        const std::ptrdiff_t len = std::min(lt::default_block_size, slot.size - offset);
        if ((len <= 0) || (offset + len > static_cast<std::ptrdiff_t>(slot.data.size())))
        {
            ec = lt::storage_error();
            return {};
        }

        return lt::hasher256(lt::span<const char>{slot.data.data() + offset, len}).final();
    }

    void SlidingWindowStorage::tryFlushAndEvict()
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        while (m_window.count(m_headPiece) > 0)
        {
            auto &slot = m_window[m_headPiece];
            if (!slot.verified)
                break; // Await SHA verification

            const char *ptr = slot.data.data();
            std::size_t remaining = slot.data.size();

            while (remaining > 0)
            {
                std::size_t chunk = remaining;

                // Sequential write to target files on cloud mount (zero disk cache)
                if (!m_opts.targetDirPath.empty() && (m_files.num_files() > 0))
                {
                    if (static_cast<std::int64_t>(m_totalStreamedBytes) < m_files.total_size())
                    {
                        const lt::file_index_t fIdx = m_files.file_index_at_offset(static_cast<std::int64_t>(m_totalStreamedBytes));
                        const std::int64_t fileOffset = m_files.file_offset(fIdx);
                        const std::int64_t fileSize = m_files.file_size(fIdx);
                        const std::int64_t bytesLeftInFile = (fileOffset + fileSize) - static_cast<std::int64_t>(m_totalStreamedBytes);

                        if (bytesLeftInFile <= 0)
                        {
                            if (m_currentFileFd >= 0)
                            {
                                ::close(m_currentFileFd);
                                m_currentFileFd = -1;
                            }
                            m_totalStreamedBytes = static_cast<std::uint64_t>(fileOffset + fileSize);
                            continue;
                        }
                        chunk = std::min<std::size_t>(remaining, static_cast<std::size_t>(bytesLeftInFile));

                        if (m_currentFileIdx != fIdx)
                        {
                            if (m_currentFileFd >= 0)
                            {
                                ::close(m_currentFileFd);
                                m_currentFileFd = -1;
                            }
                            m_currentFileIdx = fIdx;

                            if (!m_files.pad_file_at(fIdx) && (fileSize > 0))
                            {
                                const std::string fullFilePath = m_files.file_path(fIdx, m_opts.targetDirPath);
                                std::error_code ec;
                                std::filesystem::create_directories(std::filesystem::path(fullFilePath).parent_path(), ec);
                                m_currentFileFd = ::open(fullFilePath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
                                FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
                                if (dbg) {
                                    std::fprintf(dbg, "[StreamStorage] Opened file '%s', fd=%d, errno=%d\n", fullFilePath.c_str(), m_currentFileFd, errno);
                                    std::fclose(dbg);
                                }
                            }
                        }

                        if (m_currentFileFd >= 0)
                        {
                            std::size_t fileRemaining = chunk;
                            const char *filePtr = ptr;
                            while (fileRemaining > 0)
                            {
                                const ssize_t written = ::write(m_currentFileFd, filePtr, fileRemaining);
                                if (written <= 0)
                                {
                                    if (errno == EINTR)
                                        continue;
                                    break;
                                }
                                filePtr += written;
                                fileRemaining -= static_cast<std::size_t>(written);
                            }
                        }

                        if (bytesLeftInFile <= static_cast<std::int64_t>(chunk))
                        {
                            if (m_currentFileFd >= 0)
                            {
                                ::close(m_currentFileFd);
                                m_currentFileFd = -1;
                                FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
                                if (dbg) {
                                    std::fprintf(dbg, "[StreamStorage] Closed file index %d upon completion\n", static_cast<int>(fIdx));
                                    std::fclose(dbg);
                                }
                            }
                        }
                    }
                }

                // Write to explicit outputFd if open
                if (m_outputFd >= 0)
                {
                    std::size_t outRemaining = chunk;
                    const char *outPtr = ptr;
                    while (outRemaining > 0)
                    {
                        const ssize_t written = ::write(m_outputFd, outPtr, outRemaining);
                        if (written <= 0)
                        {
                            if (errno == EINTR)
                                continue;
                            break;
                        }
                        outPtr += written;
                        outRemaining -= static_cast<std::size_t>(written);
                    }
                }

                // Write to live stream FIFO if open (non-blocking)
                if (m_fifoFd >= 0)
                {
                    ssize_t written = ::write(m_fifoFd, ptr, chunk);
                    (void)written;
                }

                ptr += chunk;
                remaining -= chunk;
                m_totalStreamedBytes += static_cast<std::uint64_t>(chunk);
            }

            FILE *dbg = std::fopen("/tmp/stream_debug.log", "a");
            if (dbg) {
                std::fprintf(dbg, "[StreamStorage] Flushed piece=%d, totalStreamed=%llu / %lld bytes\n",
                             static_cast<int>(m_headPiece), static_cast<unsigned long long>(m_totalStreamedBytes),
                             static_cast<long long>(m_files.total_size()));
                std::fclose(dbg);
            }

            // Evict emitted piece from memory
            m_window.erase(m_headPiece);
            m_headPiece++;
        }
    }
}
