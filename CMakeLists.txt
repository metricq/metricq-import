project(metricq-import)

cmake_minimum_required(VERSION 3.8)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

include(cmake/DefaultBuildType.cmake)
include(cmake/GitSubmoduleUpdate.cmake)
git_submodule_update()

find_package(Boost COMPONENTS program_options system timer REQUIRED)
find_package(MySQLConnectorCPP REQUIRED)

add_subdirectory(lib/hta)

add_executable(hta_mysql_import src/mysql_import.cpp)

target_link_libraries(hta_mysql_import PRIVATE hta::hta ${MYSQLCONNECTORCPP_LIBRARIES}
        Boost::program_options Boost::system Boost::timer)
target_include_directories(hta_mysql_import PRIVATE ${MYSQLCONNECTORCPP_INCLUDE_DIRS})

install(TARGETS hta_mysql_import
    RUNTIME DESTINATION bin
)
