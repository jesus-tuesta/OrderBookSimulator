add_executable(app app.cc orderBookTrack.hpp networking.hpp configBars.h barAsParquet.hpp commons.hpp channel.hpp)
target_link_libraries(app PRIVATE Boost::beast OpenSSL::SSL nlohmann_json::nlohmann_json
                                        OpenSSL::Crypto)

target_link_libraries(app PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
target_link_libraries(app PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")