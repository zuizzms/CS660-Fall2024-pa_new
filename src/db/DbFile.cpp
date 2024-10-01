#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td)
    : name(name), td(td), numPages(0) {
  // Open or create the file
  int file = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (file < 0) {
    throw std::runtime_error("Unable to open or create file: " + name);
  }

  // Get the file size using fstat
  struct stat fileStat;
  if (fstat(file, &fileStat) < 0) {
    close(file);
    throw std::runtime_error("fstat failed");
  }

  // Calculate the number of pages based on the file size
  numPages = fileStat.st_size / DEFAULT_PAGE_SIZE;

  // If the file is empty, create an empty page
  if (numPages == 0) {
    numPages = 1;
    char emptyPage[DEFAULT_PAGE_SIZE] = {0};  // Initialize with zeros
    if (write(file, emptyPage, DEFAULT_PAGE_SIZE) != DEFAULT_PAGE_SIZE) {
      close(file);
      throw std::runtime_error("Failed to initialize empty page");
    }
  }

  close(file);
}

DbFile::~DbFile() {
  // Close the file (assuming file handler is stored)
  int file = open(name.c_str(), O_RDWR);
  if (file < 0) {
    throw std::runtime_error("Unable to open file for closing: " + name);
  }
  close(file);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  if (id >= numPages) {
    throw std::out_of_range("Page ID out of range");
  }

  // Open the file
  int file = open(name.c_str(), O_RDONLY);
  if (file < 0) {
    throw std::runtime_error("Unable to open file for reading: " + name);
  }

  // Calculate the byte offset for the page
  off_t offset = id * DEFAULT_PAGE_SIZE;

  // Read the page into the provided Page object
  if (pread(file, page.getData(), DEFAULT_PAGE_SIZE, offset) != DEFAULT_PAGE_SIZE) {
    close(file);
    throw std::runtime_error("Failed to read page from file");
  }

  // Close the file
  close(file);

  reads.push_back(id); // Log the read
}

void DbFile::writePage(const Page &page, const size_t id) const {
  if (id >= numPages) {
    throw std::out_of_range("Page ID out of range");
  }

  // Open the file
  int file = open(name.c_str(), O_WRONLY);
  if (file < 0) {
    throw std::runtime_error("Unable to open file for writing: " + name);
  }

  // Calculate the byte offset for the page
  off_t offset = id * DEFAULT_PAGE_SIZE;

  // Write the page data to the file
  if (pwrite(file, page.getData(), DEFAULT_PAGE_SIZE, offset) != DEFAULT_PAGE_SIZE) {
    close(file);
    throw std::runtime_error("Failed to write page to file");
  }

  // Close the file
  close(file);

  writes.push_back(id); // Log the write
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
