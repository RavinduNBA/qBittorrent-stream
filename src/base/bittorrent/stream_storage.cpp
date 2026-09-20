#include "stream_storage.hpp"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/asio/error.hpp>

#include <libtorrent/hex.hpp>

namespace BitTorrent
{
    namespace
    {
        bool writeAll(const int fd, const char *data, std::size_t size)
        {
            while (size > 0)
            {
                const ssize_t written = ::write(fd, data, size);
                if (written > 0)
                {
                    data += written;
                    size -= static_cast<std::size_t>(written);
                    continue;
                }
                if ((written < 0) && (errno == EINTR))
                    continue;
                return false;
            }
            return true;
        }
    }

    SlidingWindowStorage::SlidingWindowStorage(const lt::file_storage &fs, const StreamOptions &opts)
        : m_files {fs}
        , m_opts {opts}
    {
        std::signal(SIGPIPE, SIG_IGN);
        m_totalPieces = m_files.is_valid() ? m_files.num_pieces() : 0;
        std::error_code error;
        std::filesystem::remove_all(m_opts.spoolDir, error);
        error.clear();
        std::filesystem::create_directories(m_opts.spoolDir, error);
        m_uploaderThread = std::thread(&SlidingWindowStorage::uploaderLoop, this);
    }

    SlidingWindowStorage::~SlidingWindowStorage()
    {
        stop();
    }

    int SlidingWindowStorage::calculatePieceSize(const lt::piece_index_t piece) const
    {
        if (!m_files.is_valid())
            return 0;
        return m_files.piece_size(piece);
    }

    std::size_t SlidingWindowStorage::activeBufferedBytes() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::size_t total = 0;
        for (const auto &[piece, slot] : m_window)
            total += slot.data.size();
        return total;
    }

    lt::piece_index_t SlidingWindowStorage::headPiece() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_headPiece;
    }

    std::uint64_t SlidingWindowStorage::totalStreamedBytes() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_totalStreamedBytes;
    }

    bool SlidingWindowStorage::isWriteQueueFull() const
    {
        return activeBufferedBytes() >= m_opts.maxBufferBytes;
    }

    bool SlidingWindowStorage::hasCommittedPiece(const lt::piece_index_t piece) const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_committedPieceHashes.contains(piece);
    }

    lt::span<const char> SlidingWindowStorage::readv(const lt::peer_request &request, lt::storage_error &error)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_window.find(request.piece);
        if (it == m_window.end())
        {
            error.operation = lt::operation_t::file_read;
            error.ec = boost::asio::error::operation_aborted;
            return {};
        }

        const PieceSlot &slot = it->second;
        if (static_cast<int>(slot.data.size()) <= request.start)
        {
            error.operation = lt::operation_t::file_read;
            error.ec = boost::asio::error::operation_aborted;
            return {};
        }

        const int available = static_cast<int>(slot.data.size()) - request.start;
        return {slot.data.data() + request.start
                , static_cast<std::ptrdiff_t>(std::min(request.length, available))};
    }

    void SlidingWindowStorage::writev(const lt::span<const char> buffer, const lt::piece_index_t piece, const int offset)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (piece < m_headPiece)
            return;
        const int pieceLength = std::max(1, m_files.piece_length());
        const int windowPieces = std::max(1, static_cast<int>((m_opts.maxBufferBytes + pieceLength - 1) / pieceLength));
        if (static_cast<int>(piece) >= (static_cast<int>(m_headPiece) + windowPieces))
            return;

        PieceSlot &slot = m_window[piece];
        if (slot.data.empty())
        {
            slot.size = calculatePieceSize(piece);
            slot.data.resize(static_cast<std::size_t>(slot.size), 0);
            slot.totalBlocks = (slot.size + lt::default_block_size - 1) / lt::default_block_size;
            slot.blocksReceived.assign(static_cast<std::size_t>(slot.totalBlocks), false);
        }

        if ((offset < 0) || ((offset + buffer.size()) > static_cast<std::ptrdiff_t>(slot.data.size())))
            return;

        std::memcpy(slot.data.data() + offset, buffer.data(), static_cast<std::size_t>(buffer.size()));
        const int blockIndex = offset / lt::default_block_size;
        if ((blockIndex >= 0) && (blockIndex < slot.totalBlocks)
                && !slot.blocksReceived[static_cast<std::size_t>(blockIndex)])
        {
            slot.blocksReceived[static_cast<std::size_t>(blockIndex)] = true;
            ++slot.blocksDone;
        }
    }

    void SlidingWindowStorage::clearPiece(const lt::piece_index_t piece)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_committedPieceHashes.contains(piece))
            m_window.erase(piece);
    }

    std::optional<lt::sha1_hash> SlidingWindowStorage::committedPieceHash(const lt::piece_index_t piece) const
    {
        const auto it = m_committedPieceHashes.find(piece);
        if (it == m_committedPieceHashes.end())
            return std::nullopt;
        return it->second;
    }

    lt::sha1_hash SlidingWindowStorage::hash(const lt::piece_index_t piece
            , const lt::span<lt::sha256_hash> blockHashes, lt::storage_error &error)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (const std::optional<lt::sha1_hash> storedHash = committedPieceHash(piece))
            return *storedHash;

        const auto it = m_window.find(piece);
        if ((it == m_window.end()) || it->second.data.empty())
        {
            error = lt::storage_error();
            return {};
        }

        PieceSlot &slot = it->second;
        if (!blockHashes.empty())
        {
            const int blocksInPiece = (slot.size + lt::default_block_size - 1) / lt::default_block_size;
            const char *data = slot.data.data();
            for (int i = 0; (i < blocksInPiece) && (i < static_cast<int>(blockHashes.size())); ++i)
            {
                const std::ptrdiff_t length = std::min(lt::default_block_size, slot.size - (i * lt::default_block_size));
                blockHashes[i] = lt::hasher256(lt::span<const char> {data + (i * lt::default_block_size), length}).final();
            }
        }

        return lt::hasher(slot.data).final();
    }

    lt::sha256_hash SlidingWindowStorage::hash2(const lt::piece_index_t piece, const int offset, lt::storage_error &error)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_window.find(piece);
        if ((it == m_window.end()) || it->second.data.empty())
        {
            error = lt::storage_error();
            return {};
        }

        const PieceSlot &slot = it->second;
        const std::ptrdiff_t length = std::min(lt::default_block_size, slot.size - offset);
        if ((length <= 0) || ((offset + length) > static_cast<std::ptrdiff_t>(slot.data.size())))
        {
            error = lt::storage_error();
            return {};
        }
        return lt::hasher256(lt::span<const char> {slot.data.data() + offset, length}).final();
    }

    void SlidingWindowStorage::markPieceVerified(const lt::piece_index_t piece)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_window.find(piece);
            if (it == m_window.end())
                return;
            it->second.verified = true;
        }
        spoolVerifiedPieces();
    }

    bool SlidingWindowStorage::openRemoteFile(const lt::file_index_t file)
    {
        if (m_uploadFd >= 0)
            return m_openFile == file;

        int pipeFds[2];
        if (::pipe(pipeFds) != 0)
            return false;

        const pid_t child = ::fork();
        if (child < 0)
        {
            ::close(pipeFds[0]);
            ::close(pipeFds[1]);
            return false;
        }

        if (child == 0)
        {
            ::dup2(pipeFds[0], STDIN_FILENO);
            ::close(pipeFds[0]);
            ::close(pipeFds[1]);
            std::string filePath = m_files.file_path(file);
            static const std::string incompleteSuffix = ".!qB";
            if (filePath.ends_with(incompleteSuffix))
                filePath.erase(filePath.size() - incompleteSuffix.size());
            const std::string remotePath = m_opts.remoteBasePath + "/" + filePath;
            const std::string sizeString = std::to_string(m_files.file_size(file));
            ::execl("/usr/bin/rclone", "rclone", "rcat", remotePath.c_str()
                    , "--config", m_opts.rcloneConfigPath.c_str()
                    , "--size", sizeString.c_str()
                    , "--drive-chunk-size", "16M"
                    , "--low-level-retries", "20"
                    , static_cast<char *>(nullptr));
            _exit(127);
        }

        ::close(pipeFds[0]);
        m_uploadFd = pipeFds[1];
        m_uploadPid = child;
        m_openFile = file;
        return true;
    }

    bool SlidingWindowStorage::closeRemoteFile()
    {
        if (m_uploadFd < 0)
            return true;

        ::close(m_uploadFd);
        m_uploadFd = -1;

        int status = 0;
        while ((::waitpid(m_uploadPid, &status, 0) < 0) && (errno == EINTR)) {}
        m_uploadPid = -1;
        m_openFile = lt::file_index_t {-1};
        return WIFEXITED(status) && (WEXITSTATUS(status) == 0);
    }

    void SlidingWindowStorage::stop()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopping)
                return;
            m_stopping = true;
        }
        m_spoolCondition.notify_all();
        if (m_uploaderThread.joinable())
            m_uploaderThread.join();
        closeRemoteFile();
    }

    bool SlidingWindowStorage::writeTorrentRange(const char *data, const std::size_t size
            , const std::int64_t torrentOffset)
    {
        const std::int64_t rangeEnd = torrentOffset + static_cast<std::int64_t>(size);
        for (const lt::file_index_t file : m_files.file_range())
        {
            const std::int64_t fileStart = m_files.file_offset(file);
            const std::int64_t fileEnd = fileStart + m_files.file_size(file);
            const std::int64_t copyStart = std::max(torrentOffset, fileStart);
            const std::int64_t copyEnd = std::min(rangeEnd, fileEnd);
            if (copyStart >= copyEnd)
                continue;

            if (!m_files.pad_file_at(file))
            {
                if ((m_uploadFd >= 0) && (m_openFile != file) && !closeRemoteFile())
                    return false;
                if (!openRemoteFile(file))
                    return false;
                const std::size_t sourceOffset = static_cast<std::size_t>(copyStart - torrentOffset);
                const std::size_t copySize = static_cast<std::size_t>(copyEnd - copyStart);
                if (!writeAll(m_uploadFd, data + sourceOffset, copySize))
                {
                    closeRemoteFile();
                    return false;
                }
            }

            if ((copyEnd == fileEnd) && (m_uploadFd >= 0) && (m_openFile == file)
                    && !closeRemoteFile())
                return false;
        }
        return true;
    }

    void SlidingWindowStorage::spoolVerifiedPieces()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        while (true)
        {
            const auto it = m_window.find(m_headPiece);
            if ((it == m_window.end()) || !it->second.verified)
                break;

            const std::size_t size = it->second.data.size();
            if ((m_spoolBytes + size) > m_opts.maxSpoolBytes)
                break;

            const std::int64_t torrentOffset = static_cast<std::int64_t>(static_cast<int>(m_headPiece))
                    * m_files.piece_length();
            const std::string path = m_opts.spoolDir + "/segment-" + std::to_string(m_segmentIndex++) + ".bin";
            std::ofstream output(path, std::ios::binary | std::ios::trunc);
            output.write(it->second.data.data(), static_cast<std::streamsize>(size));
            output.close();
            if (!output)
                break;

            m_committedPieceHashes[m_headPiece] = lt::hasher(it->second.data).final();
            m_window.erase(it);
            m_spoolQueue.push_back({path, torrentOffset, size});
            m_spoolBytes += size;
            m_headPiece = lt::piece_index_t(static_cast<int>(m_headPiece) + 1);
        }
        m_spoolCondition.notify_one();
    }

    void SlidingWindowStorage::uploaderLoop()
    {
        while (true)
        {
            SpoolSegment segment;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_spoolCondition.wait(lock, [this] { return m_stopping || !m_spoolQueue.empty(); });
                if (m_spoolQueue.empty())
                    break;
                segment = m_spoolQueue.front();
            }

            std::ifstream input(segment.path, std::ios::binary);
            std::vector<char> data((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
            const bool success = (data.size() == segment.size)
                    && writeTorrentRange(data.data(), data.size(), segment.torrentOffset);
            if (!success)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                if (m_stopping)
                    break;
                m_spoolCondition.wait_for(lock, std::chrono::seconds(1));
                continue;
            }

            std::error_code error;
            std::filesystem::remove(segment.path, error);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_spoolQueue.pop_front();
                m_spoolBytes -= segment.size;
                m_totalStreamedBytes += segment.size;
            }
            spoolVerifiedPieces();
        }
    }

    void SlidingWindowStorage::tryFlushAndEvict()
    {
        spoolVerifiedPieces();
    }
}
