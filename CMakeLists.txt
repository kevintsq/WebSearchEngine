cmake_minimum_required(VERSION 3.26)
project(WebSearchEngine)

set(CMAKE_CXX_STANDARD 23)

find_package(Python3 3.12 REQUIRED COMPONENTS Development)

include_directories(${Python3_INCLUDE_DIRS})

add_executable(WebSearchEngine
        main.cpp)

# link zlibwapi.dll
#target_link_libraries(WebSearchEngine ${CMAKE_SOURCE_DIR}/zlibwapi.lib)

# link python312.lib
target_link_libraries(WebSearchEngine ${Python3_LIBRARIES})