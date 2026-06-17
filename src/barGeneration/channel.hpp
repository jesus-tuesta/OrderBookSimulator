#include <boost/asio.hpp>
#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <deque>
#include <iostream>
#include <queue>

template<typename T>
class channel {
public:
  explicit channel(boost::asio::any_io_executor ex, std::size_t capacity)
    : ex_(ex), capacity_(capacity)
  {
  }

  // SEND
  boost::asio::awaitable<void> send(T value)
  {
    // If queue is full, suspend
    if (queue_.size() >= capacity_) {
      std::cout << "Exceed capacity push out and save data" << std::endl;
      co_await boost::asio::
        async_initiate<const boost::asio::use_awaitable_t<>&, void()>(
          [this](auto handler) {
            waiting_senders_.push_back(std::move(handler));
          },
          boost::asio::use_awaitable);
    }

    std::cout << "Saving message on queue" << std::endl;
    queue_.push(std::move(value));

    // Wake one receiver if any
    if (!waiting_receivers_.empty()) {
      std::cout << "If we have any receiver, wake up and send data"
                << std::endl;
      auto h = std::move(waiting_receivers_.front());
      waiting_receivers_.pop_front();
      boost::asio::post(ex_, [h = std::move(h)]() mutable { std::move(h)(); });
    }
  }

  // RECEIVE
  boost::asio::awaitable<T> receive()
  {
    // If queue empty, suspend
    if (queue_.empty()) {
      std::cout << "Nothing on queue, send handler to waiting_receiver, and go "
                   "back to send"
                << std::endl;
      co_await boost::asio::
        async_initiate<const boost::asio::use_awaitable_t<>&, void()>(
          [this](auto handler) {
            waiting_receivers_.push_back(std::move(handler));
          },
          boost::asio::use_awaitable);
    }
    std::cout << "Take value out of channel and sent it " << std::endl;
    T value = std::move(queue_.front());
    queue_.pop();

    // Wake one sender if any
    if (!waiting_senders_.empty()) {
      auto h = std::move(waiting_senders_.front());
      std::cout << "If we have any sender, wake up and send data" << std::endl;
      waiting_senders_.pop_front();
      boost::asio::post(ex_, [h = std::move(h)]() mutable { std::move(h)(); });
    }

    co_return value;
  }

private:
  boost::asio::any_io_executor                            ex_;
  std::size_t                                             capacity_;
  std::queue<T>                                           queue_;
  std::deque<boost::asio::any_completion_handler<void()>> waiting_senders_;
  std::deque<boost::asio::any_completion_handler<void()>> waiting_receivers_;
};
