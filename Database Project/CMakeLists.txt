cmake_minimum_required(VERSION 3.10)
project(Database)

# Set policies to suppress warnings
cmake_policy(SET CMP0074 NEW)
cmake_policy(SET CMP0167 NEW)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add definitions for Windows
add_definitions(-D_WIN32_WINNT=0x0601)
add_definitions(-DNOMINMAX)
add_definitions(-D_CRT_SECURE_NO_WARNINGS)
add_definitions(-DBOOST_ASIO_HAS_STD_CHRONO)
add_definitions(-DBOOST_ASIO_HAS_STD_STRING_VIEW)

# Set Boost paths
set(BOOST_ROOT "C:/boost")  # Assuming you extracted to C:/boost
set(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
set(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

# Find required packages
find_package(Boost REQUIRED COMPONENTS system filesystem)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)

# Add source files
set(SOURCES
    main.cpp
    DatabaseManager.cpp
    query_parser.cpp
    SimpleHttpServer.cpp
    record.cpp
    page.cpp
    catalog.cpp
    bptree.cpp
)

# Add header files
set(HEADERS
    database_manager.h
    query_parser.h
    SimpleHttpServer.h
    record.h
    page.h
    catalog.h
    bptree.h
)

# Create executable
add_executable(Database ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(Database PRIVATE
    ${Boost_LIBRARIES}
    ws2_32
    mswsock
)

# Create db_data directory in the build folder
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/db_data)

# Set the working directory to the db_data directory
set_target_properties(Database PROPERTIES
    VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/db_data"
) 