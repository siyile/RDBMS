cmake_minimum_required(VERSION 2.8)
project(cs222p-winter20)

set(CMAKE_CXX_STANDARD 11)

add_custom_target(clean-all COMMAND rm Index* Indices* left* right* large* group* *out Tables Columns tbl_* *_file *idx)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -g  -fno-omit-frame-pointer -ledit")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    add_definitions(-DDEBUG=1)
endif ()

add_definitions(-DDATABASE_FOLDER=\"../cli/\")

add_library(PFM ./rbf/pfm.cc)
add_library(RBFM ./rbf/rbfm.cc)
add_library(RM ./rm/rm.cc ${RBFM})
add_library(IX ./ix/ix.cc ${PFM})
add_library(QE ./qe/qe.cc ${IX} ${RM})
add_library(CLI ./cli/cli.cc ${QE} ${IX} ${RM})


file(GLOB files rbf/rbftest*.cc)
foreach (file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} RBFM PFM)
endforeach ()

file(GLOB files ix/ixtest_*.cc)
foreach (file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} IX RM RBFM PFM)
endforeach ()

file(GLOB files rm/rmtest_*.cc)
foreach (file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} RM IX RBFM PFM)
endforeach ()

file(GLOB files qe/qetest_*.cc)
foreach (file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} QE RM IX RBFM PFM)
endforeach ()

file(GLOB files cli/cli_example_*.cc)
foreach (file ${files})
    get_filename_component(name ${file} NAME_WE)
    add_executable(${name} ${file})
    target_link_libraries(${name} CLI QE IX RM RBFM PFM)
endforeach ()

add_executable(cli_start cli/start.cc)