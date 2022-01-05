//
// Created by stewart on 12/23/21.
//

#include <cmath>
#include <fstream>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <exception>
#include <type_traits>
#include <filesystem>
#include <vector>

#ifndef MARATHON_MARATHON_HPP
#define MARATHON_MARATHON_HPP

namespace Marathon {
    namespace bip = boost::interprocess;
    
    enum class SeekReference {
        START, CURRENT, END
    };

    template<typename T> struct is_vector : public std::false_type {};
    template<typename T, typename A>
    struct is_vector<std::vector<T, A>> : public std::true_type {};

    enum class ModeType {
        READ_ONLY, READ_WRITE, READ_APPEND, TRUNCATE
    };

    class Stream {
    private:
        std::byte* buffer_start = nullptr;
//        std::byte* buffer_end = nullptr;
        size_t _size = 0;
        int64_t pos = 0;

        inline std::byte* p() {
            assert(pos >= 0 && pos <= _size);
            return buffer_start + pos;
        }

    protected:
        Stream() = default;

        inline void resize(std::byte *const bufferStart, size_t const new_size) {
            if (new_size > std::numeric_limits<int64_t>::max()) throw std::invalid_argument("Size exceeds max limit");
            if (pos > new_size) throw std::invalid_argument("Cannot resize: buffer pointer would be out of bounds");
            if (reinterpret_cast<size_t>(bufferStart) % alignof(int)) throw std::invalid_argument("Bad alignment on new stream buffer!");
            this->buffer_start = bufferStart;
//            this->buffer_end = bufferStart + new_size;
            this->_size = new_size;
        }

    public:

        template<typename T>
        void next(T *next, size_t const len) {
            static_assert(std::is_trivial_v<T>);
            size_t const nbytes = sizeof(T) * len;
            if (available() < nbytes) throw std::invalid_argument("Insufficient space in buffer for read");
            memcpy(next, p(), nbytes);
            pos += nbytes;
        }

        template<typename T>
        void next(T &v) {
            if constexpr (std::is_trivial_v<T>) {
                next(reinterpret_cast<char *>(&v), sizeof(T));
            } else if constexpr (std::is_same_v<std::string, T>) {
                next(v.data(), v.size());
            } else if constexpr (is_vector<T>::value) {
                static_assert(std::is_trivial_v<typename T::value_type>);
                next(reinterpret_cast<char*>(v.data()), sizeof(typename T::value_type) * v.size());
            } else {
                throw std::logic_error(std::string("Unsupported type ") + typeid(T).name());
            }
        }

        template<typename T>
        void next(T const * next, size_t const len) {
            static_assert(std::is_trivial_v<T>);
            size_t const nbytes = sizeof(T) * len;
            if (available() < nbytes) throw std::invalid_argument("Insufficient space in buffer for write");
            memcpy(p(), next, nbytes);
            pos += nbytes;
        }

        template<typename T>
        void next(T const &v) {
            if constexpr (std::is_trivial_v<T>) {
                next(reinterpret_cast<std::byte const*>(&v), sizeof(T));
            } else if (std::is_same_v<std::string, T>) {
                next(v.data(), v.size());
            } else if (is_vector<T>::value) {
                static_assert(std::is_trivial_v<typename T::value_type>);
                next(reinterpret_cast<char const*>(v.data()), sizeof(typename T::value_type) * v.size());
            } else {
                throw std::logic_error(std::string("Unsupported type ") + typeid(T).name());
            }
        }

        template <typename T>
        class OffsetPtr {
            Stream const* const stream = nullptr;
            int64_t const offset = 0;

        public:
            OffsetPtr(Stream* stream, size_t offset) : stream(stream), offset(offset) {
                if (stream == nullptr || offset > std::numeric_limits<int64_t>::max()) throw std::invalid_argument("Invalid OffsetPtr");
            }

            [[nodiscard]] int64_t const& get_offset() const {
                return offset;
            }

            T& operator*() const {
                assert(offset <= stream->_size);
                return *reinterpret_cast<T*>(stream->buffer_start + offset);
            }

            explicit operator T*() const {
                return stream->buffer_start + offset;
            }
        };

        template <typename T>
        void write(T const& val) {
            next<T>(val);
        }

        template <typename T>
        void read(T& out) {
            next<T>(out);
        }

        void seek(SeekReference const& ref, int64_t const& offset = 0) {
            if (ref == SeekReference::CURRENT) pos += offset;
            else if (ref == SeekReference::END) pos = static_cast<int64_t>(_size) + offset;
            else if (ref == SeekReference::START) pos = offset;
            else throw std::invalid_argument("Unrecognized enum");
            if (pos < 0 || pos > _size) {
                pos = 0;
                throw std::invalid_argument("Seek position out of bounds");
            }
        }

        [[nodiscard]] inline size_t available() const {
            return size() - pos;
        }

        [[nodiscard]] inline size_t size() const {
            return _size;
        }

        [[nodiscard]] inline bool eos() const {
            return pos >= _size;
        }

        [[nodiscard]] inline size_t offset() const {
            return pos;
        }

        [[nodiscard]] inline std::byte* begin() const {
            return buffer_start;
        }
    };

    class FileStream : public Stream {
    private:
        std::filesystem::path const path;
        bip::mode_t const mode;
        std::unique_ptr<bip::file_mapping> file_mapping{};
        std::unique_ptr<bip::mapped_region> mapped_region{};
        size_t file_size = 0;
        size_t buffer_offset = 0;
        double growth_ratio = 1.62;

        inline void resize(size_t const size) {
            assert(std::filesystem::file_size(path) == file_size);
            if (file_size == size) return;
            std::filesystem::resize_file(path, size);
            if (file_size < size) {
                std::filebuf fbuf;
                fbuf.open(path, std::ios_base::in | std::ios_base::out | std::ios_base::binary);
                fbuf.pubseekoff(static_cast<long>(size) - 1, std::ios_base::beg);
                fbuf.sputc(0);
                fbuf.close();
            }
            file_size = size;
        }

        std::tuple<std::unique_ptr<bip::file_mapping>, std::unique_ptr<bip::mapped_region>, size_t, size_t, size_t>
        open(std::filesystem::path const& fpath, bip::mode_t const& open_mode, size_t const offset = 0, size_t size = 0, size_t const min_size = 0) {
            if (size && min_size > size) throw std::invalid_argument("Min size cannot be greater than specified size!");
            if (!std::filesystem::exists(fpath)) {
                if (open_mode == bip::read_only) throw std::invalid_argument("File does not exist");
                else {
                    std::ofstream ostream(fpath);
                    if (ostream.fail()) throw std::logic_error("Failed to create file");
                    ostream.close();
                }
            }
            size_t current_file_size = std::filesystem::file_size(fpath);
            if (size == 0) size = current_file_size;
            if (size < min_size) size = min_size;
            if (current_file_size < size) {
                if (open_mode == bip::read_only) throw std::invalid_argument("File is smaller than specified size");
                else resize(size);
                current_file_size = size;
            }

            std::unique_ptr<bip::file_mapping> fileMapping = std::make_unique<bip::file_mapping>(fpath.c_str(), open_mode);
            if (!fileMapping) throw std::invalid_argument("Failed to construct file_mapping");
            std::unique_ptr<bip::mapped_region> mappedRegion = std::make_unique<bip::mapped_region>(*fileMapping, open_mode, offset, size);
            if (!mappedRegion) throw std::invalid_argument("Failed to construct mapped_region");
//            std::cout << "page size " << mapped_region->get_page_size() << std::endl;
//            std::cout << "Address % page size " << (reinterpret_cast<uint64_t>(mapped_region->get_address()) % mapped_region->get_page_size()) << std::endl;

            return std::make_tuple(std::move(fileMapping), std::move(mappedRegion), current_file_size, offset, size);
        }

        void grow(size_t const min_new_size = 0) {
            assert(mapped_region);
            mapped_region->flush(0, size(), true);
            auto target_size = static_cast<size_t>(std::ceil(static_cast<double>(file_size) * growth_ratio));
            if (target_size < min_new_size) target_size = min_new_size;
            auto const page_size = mapped_region->get_page_size();
            size_t const num_pages = target_size / page_size;
            bool const extra = target_size % page_size;
            size_t const new_size = (num_pages + extra) * page_size;
            resize(new_size);
            assert(file_size >= min_new_size && new_size == file_size);
            mapped_region = std::make_unique<bip::mapped_region>(*file_mapping, mode, buffer_offset, file_size);
        }

        inline std::byte* get_buffer_start() {
            return static_cast<std::byte*>(this->mapped_region->get_address());
        }

    public:
        FileStream(std::filesystem::path const& fpath, bip::mode_t const mode, size_t const offset = 0, size_t const size = 0, size_t const min_size = 0)
            : Stream(), path(fpath), mode(mode) {
            size_t buffer_size;
            std::tie(this->file_mapping, this->mapped_region, file_size, buffer_offset, buffer_size) = open(fpath, mode, offset, size, min_size);
            Stream::resize(get_buffer_start(), buffer_size);
        }

        void advise(bip::mapped_region::advice_types advice) {
            mapped_region->advise(advice);
        }

        void prefetch(size_t const offset = 0, size_t _size = 0) {
            static size_t constexpr MAX_PREFETCH = 100 * 1024 * 1024;
            int ret;
            if (offset == 0 && _size == 0) {
                ret = mapped_region->advise((size() > MAX_PREFETCH) ? bip::mapped_region::advice_sequential : bip::mapped_region::advice_willneed);
            } else {
                if (_size == 0) _size = size();
                size_t const page_size = mapped_region->get_page_size();
                if (_size < page_size) return;
                size_t const n_pages = _size / page_size;
                void* a = reinterpret_cast<char*>(mapped_region->get_address()) + offset;
                bool const is_unaligned = (unsigned long) a & (page_size - 1);
                void* const aligned = (is_unaligned) ? (void *) (((unsigned long) a) & ~(page_size - 1)) : a;
                ret = posix_madvise(aligned, (n_pages + is_unaligned) * page_size, (_size > MAX_PREFETCH) ? POSIX_MADV_SEQUENTIAL : POSIX_MADV_WILLNEED);
            }
            if (ret != 0) std::cerr << "Failed madvise err=" << ret << std::endl;
        }

//        void finish() {
//            size_t const page_size = mapped_region->get_page_size();
//            size_t const n_pages = offset() / page_size;
//            if (n_pages == 0) return;
//            void* a = reinterpret_cast<char*>(mapped_region->get_address());
//            bool const is_unaligned = (unsigned long) a & (page_size - 1);
//            void* aligned = (is_unaligned) ? (void *) (((unsigned long) a) & ~(page_size - 1)) : a;
//            auto const ret = posix_madvise(aligned, (n_pages + is_unaligned) * page_size, POSIX_FADV_DONTNEED);
//            if (ret != 0) std::cerr << "Failed madvise!" << std::endl;
//        }

        void prepareWrite(size_t const write_size) {
            if (mode == bip::read_only) throw std::logic_error("Cannot prepare for write on read only file");
            auto const overflow = static_cast<int64_t>(write_size - available());
            if (overflow > 0) {
                auto const buffer = file_size - size();
                if (overflow > buffer) grow(size() + write_size);
                Stream::resize(get_buffer_start(), size() + write_size);
                assert(available() >= write_size);
            }
            prefetch(offset(), write_size);
        }

        void prepareRead(size_t const read_size = 0) {
            prefetch(offset(), read_size);
        }

        bool truncate(size_t const truncate) {
            assert(mapped_region);
            if (truncate >= file_size) return false;
            if (truncate <= offset()) throw std::invalid_argument("Cannot truncate before current offset!");
            bool shrink_map = truncate < size();
            if (shrink_map || truncate == size()) mapped_region->flush(0, truncate, false);
            resize(truncate);
            mapped_region = std::make_unique<bip::mapped_region>(*file_mapping, mode, buffer_offset, file_size);
            if (shrink_map) Stream::resize(get_buffer_start(), truncate);
            return true;
        }

        ~FileStream() {
            if (mapped_region->get_mode() == bip::read_write) {
                resize(size());
//                mapped_region->flush(0, size(), false);
            }
        }
    };

    namespace impl {
        struct ArchivePointer {
            size_t offset;
            size_t size;
        };

        struct InplaceData {
            int8_t negsize;
            std::byte data[sizeof(ArchivePointer) - sizeof(negsize)];
        };

        template<template<class> class Serializer>
        struct CountingStream {
            size_t count = 0;
            size_t size = 0;

            template<typename T>
            void constexpr next(T const &v) {
                if constexpr(std::is_trivial_v<T>) {
                    count++;
                    size += sizeof(T);
                } else if constexpr(std::is_same_v<T, std::string> || is_vector<T>::value) {
                    count++;
                    static_assert(sizeof(ArchivePointer) == sizeof(InplaceData));
                    size += sizeof(ArchivePointer);
                } else {
                    Serializer<T>::write(*this, v);
                }
            }
        };

        template<class RowType, template<class> class Serializer>
        struct Description {
            static size_t get_length() {
                CountingStream<Serializer> stream;
                Serializer<RowType>::write(stream, RowType());
                return stream.count;
            }

            static size_t get_size() {
                CountingStream<Serializer> stream;
                Serializer<RowType>::write(stream, RowType());
                return stream.size;
            }
        };
    }
    using namespace impl;

    /**
     * Table File Format:
     * uint32_t VERSION
     * uint32_t row_length
     * uint64_t row_count
     * uint32_t row_size
     * uint32_t prologue_size
     * byte[prologue_size] prologueStart
     * uint16_t[row_length] type_flags (high byte) size (low byte)
     * bytes[row_size * row_count] tableStart
     *
     * Data file format:
     * uint64_t data_len
     * byte[data_len] data
     *
     * @tparam RowType
     */
    template <typename RowType, template<class> class Serializer>
    class TableArchive {
        static size_t constexpr VERSION = 0x00001;
        static bool check_compatibility(size_t const& version) {
            return version == VERSION;
        }

        const size_t ROW_LENGTH = Description<RowType, Serializer>::get_length();
        const size_t ROW_SIZE = Description<RowType, Serializer>::get_size();
        const size_t TYPE_HEADER_SIZE = ROW_LENGTH * sizeof(uint16_t);
        size_t const MIN_TABLE_SIZE = 24 + TYPE_HEADER_SIZE;
        size_t const MIN_DATA_SIZE = sizeof(uint64_t);

        std::filesystem::path base_dir;
        std::string name;
        FileStream tableStream;
        FileStream dataStream;

        Stream::OffsetPtr<uint32_t> const version = {&tableStream, 0};
        Stream::OffsetPtr<uint32_t> const row_length = {&tableStream, 4};
        Stream::OffsetPtr<uint64_t> const row_count = {&tableStream, 8};
        Stream::OffsetPtr<uint32_t> const row_size = {&tableStream, 16};
        Stream::OffsetPtr<uint32_t> const prologue_size = {&tableStream, 20};
        Stream::OffsetPtr<uint16_t> const type_header = {&tableStream, 24};
        Stream::OffsetPtr<std::byte> const prologueStart = {&tableStream, 24 + TYPE_HEADER_SIZE};
        Stream::OffsetPtr<std::byte> const tableStart;

        Stream::OffsetPtr<uint64_t> const data_size = {&dataStream, 0};
        Stream::OffsetPtr<std::byte> const dataStart = {&dataStream, 8};

        std::vector<uint16_t> getTypeHeader() {
            return std::vector<uint16_t>(*row_length);
        }

        static std::filesystem::path setupDir(std::string const& name) {
            std::filesystem::path dpath(name);
            if (std::filesystem::exists(dpath)) {
                if (!std::filesystem::is_directory(dpath))
                    throw std::invalid_argument(name + " exists and is not a directory");
            } else {
                std::filesystem::create_directories(dpath);
            }
            return dpath;
        }

        template <typename T>
        void writeFixed(T const& v) {
            try {
                static_assert(std::is_trivial_v<T>);
                tableStream.write(v);
            } catch (std::invalid_argument const& ex) {
                throw std::invalid_argument(std::string("Failed fixed write to Table: ") + ex.what());
            }
        }

        template <typename T>
        void readFixed(T& v) {
            try {
                static_assert(std::is_trivial_v<T>);
                tableStream.read(v);
            } catch (std::invalid_argument const& ex) {
                throw std::invalid_argument(std::string("Failed fixed read from Table: ") + ex.what());
            }
        }

        template <typename T>
        void writeDynamic(T const* const v, size_t const len) {
            dataStream.seek(SeekReference::START, dataStart.get_offset() + (*data_size));
            size_t const write_size = sizeof(T) * len;
            static_assert(sizeof(InplaceData::data) <= sizeof(ArchivePointer));
            if (write_size >= sizeof(InplaceData::data)) {
                size_t const offset = dataStream.offset();
                dataStream.prepareWrite(write_size);
                try {
                    dataStream.next(v, len);
                } catch (std::invalid_argument const &ex) {
                    throw std::invalid_argument(std::string("Failed dynamic write to Archive: ") + ex.what());
                }
                size_t const new_offset = dataStream.offset();
                if (new_offset - offset != write_size)
                    throw std::runtime_error("Failed dynamic write to Archive: unexpected write size");
                *data_size = new_offset - dataStart.get_offset();
//                dataStream.finish();
                writeFixed(htobe64(offset));
                writeFixed(htobe64(len));
            } else {
                int8_t const size_indicator = -static_cast<int8_t>(write_size);
                tableStream.write(size_indicator);
                tableStream.next(v, len);
                tableStream.seek(SeekReference::CURRENT, sizeof(ArchivePointer) - (len + 1));
            }
        }

        template <typename T>
        void readDynamic(T& v) {
            ArchivePointer archivePointer{0, 0};
            tableStream.read(archivePointer);
            size_t const offset = be64toh(archivePointer.offset);
            if (offset <= INT64_MAX) {
                size_t const size = be64toh(archivePointer.size);
                assert(offset >= sizeof(uint64_t));
                if (offset > std::numeric_limits<int64_t>::max()) throw std::runtime_error("Invalid offset for read!");
                if constexpr(std::is_same_v<std::string, T>) {
                    v.resize(size);
                } else {
                    assert(size % sizeof(typename T::value_type) == 0);
                    v.resize(size / sizeof(typename T::value_type));
                }
                dataStream.seek(SeekReference::START, static_cast<int64_t>(offset));
                dataStream.prepareRead(size);
                try {
                    dataStream.next(reinterpret_cast<char*>(v.data()), size);
                } catch (std::invalid_argument const& ex) {
                    throw std::invalid_argument(std::string("Failed dynamic write to Archive: ") + ex.what());
                }
            } else {
                auto const inplaceData = reinterpret_cast<InplaceData*>(&archivePointer);
                int8_t const size = -(inplaceData->negsize);
                assert(size > 0);
                if constexpr(std::is_same_v<std::string, T>) {
                    v.resize(size);
                } else {
                    assert(size % sizeof(typename T::value_type) == 0);
                    v.resize(size / sizeof(typename T::value_type));
                }
                memcpy(v.data(), inplaceData->data, size);
            }
        }

        void initialize_new() {
            assert(MIN_TABLE_SIZE >= 4 && *version == 0);
            if (memcmp(tableStream.begin(), tableStream.begin() + 1, MIN_TABLE_SIZE - 1) != 0) {
                throw std::runtime_error("Invalid table to initialize -- nonzero bytes found");
            }
            *version = VERSION;
            *row_length = ROW_LENGTH;
            *row_count = 0;
            *row_size = ROW_SIZE;
            *prologue_size = 0;
            *data_size = 0;
        }

        [[nodiscard]] int64_t get_table_offset() const {
            return prologueStart.get_offset() + *prologue_size;
        }

    public:
        TableArchive(std::string const& directory, std::string const& name, ModeType const& mode, size_t const tableGrowthRate = 32 * 1024, size_t const dataStreamGrowthRate = 32 * 1024)
            : base_dir(setupDir(directory))
            , name(name)
            , tableStream(base_dir / (name + ".mth"), (mode == ModeType::READ_ONLY) ? bip::read_only : bip::read_write, 0, 0, MIN_TABLE_SIZE)
            , dataStream(base_dir / (name + ".dat"), (mode == ModeType::READ_ONLY) ? bip::read_only : bip::read_write, 0, 0, MIN_DATA_SIZE)
            , tableStart(&tableStream, get_table_offset()) {
            if (*version == 0) {
                initialize_new();
                tableStream.seek(SeekReference::START, type_header.get_offset());
                tableStream.write(getTypeHeader());
            } else if (!check_compatibility(*version)) throw std::invalid_argument("Unrecognized version " + std::to_string(*version));

            if (mode == ModeType::READ_WRITE || mode == ModeType::TRUNCATE) {
                *row_count = 0;
                *data_size = 0;
                if (mode == ModeType::TRUNCATE) {
                    tableStream.truncate(tableStart.get_offset());
                    dataStream.truncate(dataStart.get_offset());
                }
            }

            if (mode == ModeType::READ_APPEND) {
                tableStream.seek(SeekReference::START, tableStart.get_offset() + (*row_count) * ROW_SIZE);
            } else {
                tableStream.seek(SeekReference::START, tableStart.get_offset());
            }

            tableStream.advise(bip::mapped_region::advice_sequential);
            dataStream.advise(bip::mapped_region::advice_sequential);
        }

        [[nodiscard]] bool validate() const {
            return Description<RowType, Serializer>::get_length() == *row_length
                   && Description<RowType, Serializer>::get_size() == *row_size;
        }

        template <typename T>
        void next(T& v) {
            if constexpr(std::is_trivial_v<T>) readFixed(v);
            else if constexpr(std::is_same_v<std::string, T> || is_vector<T>::value) readDynamic(v);
            else Serializer<T>::read(*this, v);
        }

        template <typename T>
        void next(T const& v) {
            if constexpr(std::is_trivial_v<T>) writeFixed(v);
            else if constexpr(std::is_same_v<std::string, T> || is_vector<T>::value) writeDynamic(v.data(), v.size());
            else Serializer<T>::write(*this, v);
        }

        [[nodiscard]] size_t idx() const {
            auto const dist = (tableStream.offset() - tableStart.get_offset());
            auto const idx = dist / ROW_SIZE;
            assert(dist % ROW_SIZE == 0);
            return idx;
        }

        void seek(int64_t const& offset, SeekReference const& ref = SeekReference::CURRENT) {
            auto const old_idx = idx();
//            auto const new_idx = old_idx + offset;
//            if (new_idx < 0 || new_idx > *row_count) throw std::invalid_argument("Seek out of bounds");
            auto const dist = offset * ROW_SIZE;
            tableStream.seek(ref, (ref == SeekReference::START) ? (dist + tableStart.get_offset()) : dist);
        }

        uint64_t write(RowType const& row) {
            size_t const start = tableStream.offset();
            tableStream.prepareWrite(ROW_SIZE);
            auto const write_idx = *row_count;
            Serializer<RowType>::write(*this, row);
            (*row_count)++;
            size_t const final = tableStream.offset();
            if (final - start != ROW_SIZE) throw std::runtime_error("Table write size " + std::to_string(final - start) + " did not match row size " + std::to_string(ROW_SIZE));
            return write_idx;
        }

        void read(RowType& row) {
            size_t const start = tableStream.offset();
            Serializer<RowType>::read(*this, row);
            size_t const final = tableStream.offset();
            assert(final - start == ROW_SIZE);
        }

        RowType read() {
            size_t const start = tableStream.offset();
            RowType row;
            Serializer<RowType>::read(*this, row);
            size_t const final = tableStream.offset();
            assert(final - start == ROW_SIZE);
            return row;
        }

        [[nodiscard]] std::string const& get_name() const {
            return name;
        }

        [[nodiscard]] std::string get_base_dir() const {
            return base_dir.string();
        }

        [[nodiscard]] uint64_t size() const {
            return *row_count;
        }

        [[nodiscard]] bool eof() const {
            return (tableStream.offset() - tableStart.get_offset()) >= ((*row_count) * ROW_SIZE);
        }
    };
} // namespace Marathon
#endif //MARATHON_MARATHON_HPP
