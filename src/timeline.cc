#include "ps/internal/timeline.h"
#include "ps/internal/postoffice.h"
namespace ps {
void TimelineWriter::Init(int rank, int app_id, int customer_id) {
  std::string file_name = "rank_" + std::to_string(rank) + "app_id_" + std::to_string(app_id) +
                          "-customer_id_" + std::to_string(customer_id);
  file_.open(file_name, std::ios::out | std::ios::trunc);
  if (file_.good()) {
    is_running_ = true;
    writer_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&TimelineWriter::WriteLoop, this));
  } else {
    LOG(ERROR) << "Error opening PS-Lite timeline file " << file_name
               << ", will not write a timeline.";
  }
}

void TimelineWriter::EnqueueWriteRecord(const Meta& meta) {
  // CHECK_NE(meta.request_begin, 0);
  // CHECK_NE(meta.request_end, 0);
  // CHECK_NE(meta.response_begin, 0);
  // CHECK_NE(meta.response_end, 0);
  TimelineRecord tr;
  tr.sender = meta.sender;
  tr.recver = meta.recver;
  tr.ts = meta.timestamp;
  tr.queue_time = meta.response_begin - meta.request_end;
  tr.comm_time = (meta.response_end - meta.request_begin) - tr.queue_time;
  std::string ss = "";
  if (meta.push)
    ss += "push";
  if (meta.pull)
    ss += "pull";
  if (meta.request) {
    ss += "_request";
  } else {
    ss += "_response";
  }
  tr.op_name = ss;
  tr.data_size = meta.data_size;
  record_queue_.push(tr);
}

void TimelineWriter::DoWriteRecord(const TimelineRecord& tr) {
  file_ << tr.ts;
  file_ << ", " << tr.sender;
  file_ << ", " << tr.recver;
  file_ << ", " << tr.op_name;
  file_ << ", " << tr.comm_time;
  file_ << ", " << tr.queue_time;
  file_ << ", " << tr.data_size;
  file_ << std::endl;
}

void TimelineWriter::WriteLoop() {
  while (is_running_) {
    while (is_running_ && !record_queue_.empty()) {
      TimelineRecord tr = record_queue_.front();
      DoWriteRecord(tr);
      if (!file_.good()) {
        LOG(ERROR) << "Error writing to the PS-Lite timeline after it was "
                      "successfully opened, will stop writing the timeline.";
        is_running_ = false;
      }
      record_queue_.pop();
    }
  }
}

void TimelineWriter::Shutdown() {
  is_running_ = false;
  try {
    if (writer_thread_->joinable()) {
      writer_thread_->join();
    }
  } catch (const std::system_error& e) {
    LOG(INFO) << "Caught system_error while joining writer thread. Code " << e.code() << " meaning "
              << e.what();
  }
}

void Timeline::Init(int rank, int app_id, int customer_id) {
  if (is_initialized_) {
    return;
  }
  start_time_ = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "ALL START";
  // if (!Postoffice::Get()->is_worker()) {
  //   return;
  // }
  LOG(INFO) << "WORKER START";
  writer_.Init(rank, app_id, customer_id);
  is_initialized_ = writer_.IsRunning();
  CHECK_EQ(is_initialized_, true);
  LOG(INFO) << "START SUCCESS";
}

void Timeline::Write(const Meta& meta) {
  if (!is_initialized_) {
    return;
  }
  LOG(INFO) << meta.DebugString();
  writer_.EnqueueWriteRecord(meta);
}

long long Timeline::TimeSinceStartMicros() const {
  auto now = std::chrono::high_resolution_clock::now();
  auto ts = now - start_time_;
  return std::chrono::duration_cast<std::chrono::microseconds>(ts).count();
}

void Timeline::Shutdown() {
  std::lock_guard<std::recursive_mutex> lk(mu_);
  if (!is_initialized_) {
    return;
  }
  is_initialized_ = false;
  writer_.Shutdown();
}

}  // namespace ps