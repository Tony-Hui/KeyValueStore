#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <string>
#include <string_view>
#include <optional>
#include <iostream>
#include <algorithm>

#include "store/in_memory_store.h"

/**
 * @brief The Key Value Store implementation of an In Memory DB.
 */
template <typename T> class KeyValueStore : public InMemoryStore<T> {
public:
  /**
   * @brief Constructor.
   */
  KeyValueStore() {}

  /**
   * @brief Destructor.
   */
  ~KeyValueStore() override {}

  /** MAIN KEY VALUE STORE INTERACTION METHODS; Must be implemented. */
  std::optional<T> Get(std::string_view key) const override;
  void Set(std::string_view key, const T &value) override;
  void Del(std::string_view key) override;

  /** ADDITIONAL METHODS; Must be implemented. */
  std::vector<std::string>
  Keys(std::optional<T> with_value = std::nullopt) const override;
  std::vector<T> Values() const override;
  void Show(uint32_t max_records = 100) const override;
  uint32_t Count(std::optional<T> with_value = std::nullopt) const override;

  /** TRANSACTION SUPPORT METHODS; Unique to this impl */
  void Begin();
  void Commit();
  void Rollback();
private:
    /**
     * @brief Builds the final (visible) key->value map by applying
     * each transaction diff (from bottom to top) on top of the base map.
     */
    std::unordered_map<std::string, T> BuildVisibleState() const;

    // Base store of committed data (no transaction).
    std::unordered_map<std::string, T> data_;

    // Stack of diffs for each nested transaction level. The back() is the topmost.
    // Each diff is: key -> optional<T>, where std::nullopt means "deleted" at that level.
    std::vector<std::unordered_map<std::string, std::optional<T>>> transactions_;
};

/** --------------------  PRIVATE HELPER  -------------------- */
template <typename T>
std::unordered_map<std::string, T> KeyValueStore<T>::BuildVisibleState() const {
    // Start with a copy of the base (committed) data
    auto result = data_;

    // Apply each transaction layer in order (from oldest to newest)
    for (const auto &diff : transactions_) {
        for (const auto &kv : diff) {
            if (!kv.second.has_value()) {
                // Key has been deleted at this transaction level
                result.erase(kv.first);
            } else {
                // Key has been inserted/updated at this transaction level
                result[kv.first] = kv.second.value();
            }
        }
    }
    return result;
}

template <typename T>
std::optional<T> KeyValueStore<T>::Get(std::string_view key) const {
    // Search from topmost transaction down to the base
    for (auto it = transactions_.rbegin(); it != transactions_.rend(); ++it) {
        auto found = it->find(std::string(key));
        if (found != it->end()) {
            if (!found->second.has_value()) {
                return std::nullopt;
            }
            return found->second;
        }
    }
    // If not found in transactions, check the base store
    auto base_it = data_.find(std::string(key));
    if (base_it != data_.end()) {
        return base_it->second;
    }
    // Key not found
    return std::nullopt;
}

template <typename T>
void KeyValueStore<T>::Set(std::string_view key, const T &value) {
    // If we're in a transaction, record to topmost diff
    if (!transactions_.empty()) {
        transactions_.back()[std::string(key)] = value;
    } else {
        // Otherwise, directly update base store
        data_[std::string(key)] = value;
    }
}

template <typename T>
void KeyValueStore<T>::Del(std::string_view key) {
    // If we're in a transaction, record a deletion in the topmost diff
    if (!transactions_.empty()) {
        transactions_.back()[std::string(key)] = std::nullopt;
    } else {
        // Otherwise, remove it from the base store
        data_.erase(std::string(key));
    }
}

/** --------------------  TRANSACTION METHODS  -------------------- */

template <typename T>
void KeyValueStore<T>::Begin() {
    // Push a new empty diff map
    transactions_.emplace_back();
}

template <typename T>
void KeyValueStore<T>::Commit() {
    if (transactions_.empty()) {
        return;
    }

    // Take the topmost diff
    auto top_diff = std::move(transactions_.back());
    transactions_.pop_back();

    // If there's now another transaction below, merge into that
    if (!transactions_.empty()) {
        auto &below_diff = transactions_.back();
        for (auto &kv : top_diff) {
            below_diff[kv.first] = kv.second;
        }
    } else {
        // Otherwise, we merge directly into the base store
        for (auto &kv : top_diff) {
            if (!kv.second.has_value()) {
                data_.erase(kv.first);
            } else {
                data_[kv.first] = kv.second.value();
            }
        }
    }
}

template <typename T>
void KeyValueStore<T>::Rollback() {
    if (transactions_.empty()) {
        return;
    }
    // Discard the topmost diff
    transactions_.pop_back();
}

/** --------------------  ADDITIONAL METHODS  -------------------- */

template <typename T>
std::vector<std::string> KeyValueStore<T>::Keys(std::optional<T> with_value) const {
    auto state = BuildVisibleState();
    std::vector<std::string> keys;
    keys.reserve(state.size());

    for (const auto &kv : state) {
        if (with_value.has_value()) {
            if (kv.second == with_value.value()) {
                keys.push_back(kv.first);
            }
        } else {
            keys.push_back(kv.first);
        }
    }
    return keys;
}

template <typename T>
std::vector<T> KeyValueStore<T>::Values() const {
    auto state = BuildVisibleState();
    std::vector<T> vals;
    vals.reserve(state.size());

    for (const auto &kv : state) {
        vals.push_back(kv.second);
    }
    return vals;
}

template <typename T>
void KeyValueStore<T>::Show(uint32_t max_records) const {
    auto state = BuildVisibleState();
    uint32_t count = 0;
    for (const auto &kv : state) {
        if (count >= max_records) break;
        std::cout << kv.first << " : " << kv.second << "\n";
        count++;
    }
}

template <typename T>
uint32_t KeyValueStore<T>::Count(std::optional<T> with_value) const {
    auto state = BuildVisibleState();
    if (!with_value.has_value()) {
        return static_cast<uint32_t>(state.size());
    } else {
        uint32_t count = 0;
        for (const auto &kv : state) {
            if (kv.second == with_value.value()) {
                count++;
            }
        }
        return count;
    }
}

#endif // KEY_VALUE_STORE_H
