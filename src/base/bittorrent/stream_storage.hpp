#pragma once

#include <libtorrent/disk_interface.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/hasher.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/units.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace lt = libtorrent;

namespace BitTorrent
{
    struct StreamOptions
    {
        int outputFd = -1;
        std::string outputPath = "";
        std::string fifoPath = "";
        std::string targetDirPath = "";
        std::size_t maxBufferBytes = 64 * 1024 * 1024; // 64 MiB RAM sliding window
        bool closeFdOnExit = false;
    };

    struct PieceSlot
    {
        std::vector<char> data;
        std::vector<bool> blocksReceived;
        int totalBlocks = 0;
        int blocksDone = 0;
        bool verified = false;
        int size = 0;
    };

    class SlidingWindowStorage
    {
    public:
        SlidingWindowStorage(const lt::file_storage &fs, const StreamOptions &opts);
        ~SlidingWindowStorage();

        lt::span<const char> readv(const lt::peer_request &r, lt::storage_error &ec);
        void writev(lt::span<const char> b, lt::piece_index_t piece, int offset);
        lt::sha1_hash hash(lt::piece_index_t piece, lt::span<lt::sha256_hash> blockHashes, lt::storage_error &ec);
        lt::sha256_hash hash2(lt::piece_index_t piece, int offset, lt::storage_error &ec);
        void clearPiece(lt::piece_index_t piece);

        void tryFlushAndEvict();
        bool isWriteQueueFull() const;

        lt::piece_index_t headPiece() const { return m_headPiece; }
        std::size_t activeBufferedBytes() const;
        std::uint64_t totalStreamedBytes() const { return m_totalStreamedBytes; }
        int totalPieces() const { return m_totalPieces; }

    private:
        int calculatePieceSize(lt::piece_index_t piece) const;

        lt::file_storage m_files;
        StreamOptions m_opts;

        mutable std::mutex m_mutex;
        lt::piece_index_t m_headPiece {0};
        int m_totalPieces = 0;
        std::map<lt::piece_index_t, PieceSlot> m_window;

        std::uint64_t m_totalStreamedBytes = 0;
        int m_outputFd = -1;
        int m_fifoFd = -1;

        lt::file_index_t m_currentFileIdx {-1};
        int m_currentFileFd = -1;
    };
}
