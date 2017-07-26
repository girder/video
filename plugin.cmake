###############################################################################
#  Copyright Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

add_python_style_test(
  python_static_analysis_video
  "${CMAKE_CURRENT_LIST_DIR}/server"
)
add_python_style_test(
  python_static_analysis_video_tests
  "${CMAKE_CURRENT_LIST_DIR}/plugin_tests"
)

add_eslint_test(
  js_static_analysis_video_source
  "${CMAKE_CURRENT_LIST_DIR}/web_client"
  ESLINT_CONFIG_FILE "${CMAKE_CURRENT_LIST_DIR}/.eslintrc"
  ESLINT_IGNORE_FILE "${CMAKE_CURRENT_LIST_DIR}/.eslintignore"
)
add_eslint_test(
  js_static_analysis_video_tests
  "${CMAKE_CURRENT_LIST_DIR}/plugin_tests/client"
  ESLINT_CONFIG_FILE "${CMAKE_CURRENT_LIST_DIR}/plugin_tests/client/.eslintrc"
)

add_python_test(cache PLUGIN video BIND_SERVER)

# add_web_client_test(
#   video
#   "${CMAKE_CURRENT_LIST_DIR}/plugin_tests/client/test-xyz.js"
#   PLUGIN video)
#
#add_puglint_test(
#  video
#  "${CMAKE_CURRENT_LIST_DIR}/web_client/templates")
