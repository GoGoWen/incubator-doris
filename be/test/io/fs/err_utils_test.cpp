#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/logging.h"

// Mocking the HDFS C functions
char* mock_root_cause = nullptr;
char* mock_stack_trace = nullptr;

// Declare the mock functions that will be used to replace the real HDFS functions.
// These declarations must be visible before including err_utils.cpp.
extern "C" {
char* mock_hdfsGetLastExceptionRootCause();
char* mock_hdfsGetLastExceptionStackTrace();
}

// Redefine HDFS function names to use the mock implementations
#define hdfsGetLastExceptionRootCause mock_hdfsGetLastExceptionRootCause
#define hdfsGetLastExceptionStackTrace mock_hdfsGetLastExceptionStackTrace

// Include the .cpp file to be tested.
// Now, when the compiler processes this file, it will see the declarations
// for the mock functions and replace the calls accordingly.
#include "io/fs/err_utils.cpp"

// Provide the definitions for the mock functions.
// The linker will find these definitions to resolve the symbols.
extern "C" {
char* mock_hdfsGetLastExceptionRootCause() {
    return mock_root_cause;
}

char* mock_hdfsGetLastExceptionStackTrace() {
    return mock_stack_trace;
}
}

namespace doris::io {

// A LogSink to capture log messages for verification
class LogCapturer : public google::LogSink {
public:
    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line, const struct ::tm* tm_time,
              const char* message, size_t message_len) override {
        if (severity == google::WARNING) {
            captured_warnings.emplace_back(message, message_len);
        }
    }
    std::vector<std::string> captured_warnings;
};

class HdfsErrorTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_root_cause = nullptr;
        mock_stack_trace = nullptr;
    }
};

TEST_F(HdfsErrorTest, NoError) {
    ASSERT_EQ("", hdfs_error());
}

TEST_F(HdfsErrorTest, OnlyRootCause) {
    mock_root_cause = const_cast<char*>("Root cause error message");
    std::string expected = "reason: Root cause error message";
    ASSERT_EQ(expected, hdfs_error());
}

TEST_F(HdfsErrorTest, OnlyStackTrace) {
    LogCapturer log_capturer;
    google::AddLogSink(&log_capturer);

    mock_stack_trace = const_cast<char*>("Stack trace message");
    // The stack trace is logged, so the function should return an empty string
    ASSERT_EQ("", hdfs_error());

    google::RemoveLogSink(&log_capturer);

    ASSERT_EQ(1, log_capturer.captured_warnings.size());
    ASSERT_NE(std::string::npos,
              log_capturer.captured_warnings[0].find("HDFS stack trace: Stack trace message"));
}

TEST_F(HdfsErrorTest, BothRootCauseAndStackTrace) {
    LogCapturer log_capturer;
    google::AddLogSink(&log_capturer);

    mock_root_cause = const_cast<char*>("Root cause error message");
    mock_stack_trace = const_cast<char*>("Stack trace message");
    std::string expected = "reason: Root cause error message";
    ASSERT_EQ(expected, hdfs_error());

    google::RemoveLogSink(&log_capturer);

    ASSERT_EQ(1, log_capturer.captured_warnings.size());
    ASSERT_NE(std::string::npos,
              log_capturer.captured_warnings[0].find("HDFS stack trace: Stack trace message"));
}

} // namespace doris::io
