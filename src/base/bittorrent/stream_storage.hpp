#pragma once

#include <libtorrent/disk_interface.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/hasher.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/units.hpp>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace lt = libtorrent;

namespace BitTorrent
{
    struct StreamOptions
    {
        std::string remoteBasePath;
        std::string infoHash;
        std::string manifestDir;
        std::string rcloneConfigPath;
        std::string spoolDir;
        std::size_t maxBufferBytes = 64 * 1024 * 1024;
        std::uint64_t maxSpoolBytes = 4ULL * 1024 * 1024 * 1024;
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
        void markPieceVerified(lt::piece_index_t piece);
        void stop();

        void tryFlushAndEvict();
        bool isWriteQueueFull() const;
        bool hasCommittedPiece(lt::piece_index_t piece) const;

        lt::piece_index_t headPiece() const;
        std::size_t activeBufferedBytes() const;
        std::uint64_t totalStreamedBytes() const;
        int totalPieces() const { return m_totalPieces; }

    private:
        int calculatePieceSize(lt::piece_index_t piece) const;
        struct SpoolSegment
        {
            std::string path;
            std::int64_t torrentOffset = 0;
            std::size_t size = 0;
        };

        void spoolVerifiedPieces();
        void uploaderLoop();
        bool writeTorrentRange(const char *data, std::size_t size, std::int64_t torrentOffset);
        bool openRemoteFile(lt::file_index_t file);
        bool closeRemoteFile();
        std::optional<lt::sha1_hash> committedPieceHash(lt::piece_index_t piece) const;

        lt::file_storage m_files;
        StreamOptions m_opts;

        mutable std::mutex m_mutex;
        lt::piece_index_t m_headPiece {0};
        int m_totalPieces = 0;
        std::map<lt::piece_index_t, PieceSlot> m_window;
        std::map<lt::piece_index_t, lt::sha1_hash> m_committedPieceHashes;

        std::uint64_t m_totalStreamedBytes = 0;
        std::uint64_t m_spoolBytes = 0;
        std::uint64_t m_segmentIndex = 0;
        std::deque<SpoolSegment> m_spoolQueue;
        std::condition_variable m_spoolCondition;
        std::thread m_uploaderThread;
        bool m_stopping = false;
        lt::file_index_t m_openFile {lt::file_index_t {-1}};
        int m_uploadFd = -1;
        int m_uploadPid = -1;
    };
}
