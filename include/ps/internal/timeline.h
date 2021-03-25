#ifndef PS_INTERNAL_TIMELINE_H_
#define PS_INTERNAL_TIMELINE_H_
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <boost/lockfree/spsc_queue.hpp>

#include "ps/internal/message.h"
#include "ps/internal/threadsafe_queue.h"
namespace ps {

struct TimelineRecord {
  int key;
  std::string op_name;
  uint64_t comm_time;
  uint64_t queue_time;
  uint32_t ts;
  uint32_t sender;
  uint32_t recver;
  uint32_t data_size;
  DataType data_type;
};

class TimelineWriter {
 public:
  void Init(int rank, int app_id, int customer_id);
  inline bool IsRunning() const { return is_running_; }
  void EnqueueWriteRecord(const Meta&);
  void Shutdown();

 private:
  void DoWriteRecord(const TimelineRecord& tr);
  void WriteLoop();
  std::ofstream file_;
  std::atomic<bool> is_running_{false};
  boost::lockfree::spsc_queue<TimelineRecord, boost::lockfree::capacity<1048576>> record_queue_;
  std::unique_ptr<std::thread> writer_thread_;
};

class Timeline {
 public:
  void Init(int rank, int app_id, int customer_id);
  void Write(const Meta& meta);
  long long TimeSinceStartMicros() const;
  void Shutdown();
  bool IsInitialized() const { return is_initialized_; }

 private:
  bool is_initialized_{false};
  TimelineWriter writer_;
  std::chrono::high_resolution_clock::time_point start_time_;
  std::recursive_mutex mu_;
};
}  // namespace ps

#endif