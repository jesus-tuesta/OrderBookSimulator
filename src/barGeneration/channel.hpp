#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

struct thread_info {
  std::chrono::steady_clock::time_point ts;
  std::int32_t                          consumer_run;
  std::int32_t                          producer_run;
  std::size_t                           queue_size;
};

template<typename Derived, typename T>
class channel_interface {
public:
  explicit channel_interface(std::size_t capacity) : capacity_(capacity)
  {
  }
  void send(T value)
  {
    static_cast<Derived*>(this)->send_impl(std::move(value));
  }
  bool receive(T& out)
  {
    return static_cast<Derived*>(this)->receive_impl(out);
  }
  thread_info current_status()
  {
    return static_cast<Derived*>(this)->current_status_impl();
  }
  void close()
  {
    static_cast<Derived*>(this)->close_impl();
  }
  std::size_t size()
  {
    return static_cast<Derived*>(this)->size_impl();
  }
  bool empty()
  {
    return static_cast<Derived*>(this)->empty_impl();
  }

protected:
  std::size_t capacity_;
};

template<typename T>
class channel_lock_based : public channel_interface<channel_lock_based<T>, T> {
public:
  channel_lock_based(std::size_t capacity)
    : channel_interface<channel_lock_based<T>, T>(capacity)
  {
  }
  // SEND
  void send_impl(T value)
  {
    std::unique_lock<std::mutex> lock{mtx};
    not_full.wait(
      lock, [this] { return queue_.size() < this->capacity_ || done; });
    if (done)
      return;
    queue_.push(std::move(value));
    not_empty.notify_one();
    producer_count++;
  }

  // RECEIVE
  bool receive_impl(T& out)
  {
    std::unique_lock<std::mutex> lock{mtx};
    not_empty.wait(lock, [this] { return !queue_.empty() || done; });
    if (queue_.empty() && done)
      return false;

    out = std::move(queue_.front());
    queue_.pop();
    not_full.notify_one();
    consumer_count++;

    return true;
  }
  thread_info current_status_impl()
  {
    return thread_info{
      std::chrono::steady_clock::now(),
      consumer_count,
      producer_count,
      this->size_impl()};
  }

  void close_impl()
  {
    std::lock_guard<std::mutex> lock{mtx};
    done = true;
    not_full.notify_all();
    not_empty.notify_all();
  }

  std::size_t size_impl()
  {
    std::lock_guard<std::mutex> lock{mtx};
    return queue_.size();
  }
  bool empty_impl()
  {
    std::lock_guard<std::mutex> lock{mtx};
    return queue_.empty();
  }

private:
  std::queue<T>           queue_;
  std::condition_variable not_full;
  std::condition_variable not_empty;
  bool                    done{false};
  std::mutex              mtx;
  std::atomic<int>        producer_count;
  std::atomic<int>        consumer_count;
};

template<typename T>
class channel_lock_free : public channel_interface<channel_lock_free<T>, T> {
public:
  channel_lock_free(std::size_t capacity)
    : channel_interface<channel_lock_free<T>, T>(capacity)
  {
    buffer.resize(capacity);
  }

  // SEND — SPSC producer
  void send_impl(T value)
  {
    while (tail.load(std::memory_order_relaxed)
             - head.load(std::memory_order_acquire)
           >= static_cast<std::ptrdiff_t>(this->capacity_)) {
      if (done.load(std::memory_order_acquire))
        return;
      std::this_thread::yield();
    }
    auto slot    = tail.load(std::memory_order_relaxed) % this->capacity_;
    buffer[slot] = std::move(value);
    tail.fetch_add(1, std::memory_order_release);
    producer_count++;
  }

  // RECEIVE — SPSC consumer
  bool receive_impl(T& out)
  {
    while (head.load(std::memory_order_relaxed)
           == tail.load(std::memory_order_acquire)) {
      if (done.load(std::memory_order_acquire))
        return false;
      std::this_thread::yield();
    }
    auto slot = head.load(std::memory_order_relaxed) % this->capacity_;
    out       = std::move(buffer[slot]);
    head.fetch_add(1, std::memory_order_release);
    consumer_count++;
    return true;
  }

  thread_info current_status_impl()
  {
    return thread_info{
      std::chrono::steady_clock::now(),
      consumer_count.load(std::memory_order_relaxed),
      producer_count.load(std::memory_order_relaxed),
      tail.load(std::memory_order_relaxed)
        - head.load(std::memory_order_relaxed)};
  }

  void close_impl()
  {
    done.store(true, std::memory_order_relaxed);
  }

private:
  std::vector<T>       buffer;
  std::atomic<size_t>  head{0};
  std::atomic<size_t>  tail{0};
  std::atomic<bool>    done{false};
  std::atomic<int32_t> producer_count{};
  std::atomic<int32_t> consumer_count{};
};
