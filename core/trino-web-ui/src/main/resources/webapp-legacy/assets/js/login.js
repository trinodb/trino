/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

$(document).ready(function () {
    var hidePassword = $('#hide-password').text().toLowerCase() === 'true';

    var redirectPath = window.location.search.substring(1);
    if (redirectPath.indexOf('&') !== -1) {
        redirectPath = redirectPath.substring(0, redirectPath.indexOf('&'));
    }
    $("#redirectPath").val(redirectPath);

    if (hidePassword) {
        $("#password")
                .prop('required', false)
                .prop('placeholder', 'Password not allowed')
                .prop('readonly', true);
    }
    $("#login").show();
    $("#username").focus().val("");
});
