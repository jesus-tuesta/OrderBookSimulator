cmake_minimum_required(VERSION 3.22)

set(VCPKG_HOME "$ENV{VCPKG_ROOT}")
string(REPLACE "\\" "/" VCPKG_HOME "${VCPKG_HOME}")
set(CMAKE_TOOLCHAIN_FILE
    "${VCPKG_HOME}/scripts/buildsystems/vcpkg.cmake"
    CACHE STRING "")

project(OrderBook VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD          20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS        ON)

find_package(OpenSSL CONFIG REQUIRED)
find_package(boost_beast REQUIRED)

# Find Arrow[Parquet]
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# Define _WIN32_WINNT based on the Windows SDK version being used, from
# https://stackoverflow.com/a/40217291
if(WIN32)
  macro(get_WIN32_WINNT version)
    if(CMAKE_SYSTEM_VERSION)
      set(ver ${CMAKE_SYSTEM_VERSION})
      string(REGEX MATCH "^([0-9]+).([0-9])" ver ${ver})
      string(REGEX MATCH "^([0-9]+)" verMajor ${ver})
      # Check for Windows 10, b/c we'll need to convert to hex 'A'.
      if("${verMajor}" MATCHES "10")
        set(verMajor "A")
        string(REGEX REPLACE "^([0-9]+)" ${verMajor} ver ${ver})
      endif()
      # Remove all remaining '.' characters.
      string(REPLACE "." "" ver ${ver})
      # Prepend each digit with a zero.
      string(REGEX REPLACE "([0-9A-Z])" "0\\1" ver ${ver})
      set(${version} "0x${ver}")
    endif()
  endmacro()

  get_win32_winnt(ver)
  add_definitions(-D_WIN32_WINNT=${ver})
endif()

if(MSVC)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()

option(ENABLE_WARNINGS "Enable compiler warnings for a target" ON)

option(ENABLE_SANITIZE_ADDR "Enable address sanitizer" OFF)
option(ENABLE_SANITIZE_UNDEF "Enable undefined sanitizer" OFF)

set(LIBRARY_NAME Library)
set(EXECUTABLE_NAME Executable)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/")
include(Docs)
include(AddGitSubmodule)

add_git_submodule(external/json)
   
if(ENABLE_WARNINGS)
    include(Warnings)
endif()

if(ENABLE_SANITIZE_ADDR OR ENABLE_SANITIZE_UNDEF)
    include(Sanitizer)
    add_sanitizer_flags(ENABLE_SANITIZE_ADDR ENABLE_SANITIZE_UNDEF)
endif()

add_subdirectory(configured)
add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(external)
add_subdirectory(data)

if(ENABLE_TESTING)
    include(CTest)
    enable_testing()
    add_subdirectory(tests)
endif()