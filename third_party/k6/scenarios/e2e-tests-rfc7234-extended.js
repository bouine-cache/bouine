/*
 * Copyright 2022 Théotime Levêque
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
export function rfc7234Extended() {
    // First simple GET request on /sam_200
    // Should result in non-cached response

    // First simple GET request WITH DIFFERENT REQUEST HEADER on /sam_200
    // Should result in a non-cached response

    // Second simple GET request on /sam_200
    // Should result in cached response

    // Second simple GET request WITH DIFFERENT REQUEST HEADER on /sam_200
    // Should result in a cached response
    // Different requests shouldn't serve same cache shouldn't be cached

    // TESTS RELATED TO SECONDARY KEY HERE
    // Vary on specific header already cached should be served
    // if one or more fields in the vary are not matching incoming request, cache shouldn't be served

    // Check age resfreshed when hitting

    // Don't serve stale content (by default, if not disconnected)

    // Expires to be ignored if Cache-Control headers are present

    // Test for each Cache-Control header and values

    // Test for pragma: no-cache (same behavior as Cache-Control: no-cache)

    // In case of conflict between pragme & cache-control, cache-control is preferred

    // Test Warnings

    // Test Cache Control extensions

    // Test HTTP conditional request

    // THESE SHOULD BE CONFIGURABLE OPTIONS

    // 206 status code (Partial Content)
    // Should result in a non-cached response

    // 206 status code (Partial Content) again
    // Should result in a non-cached response

    // 404 status code (Partial Content)
    // Should result in a non-cached response

    // 404 status code (Partial Content) again
    // Should result in a non-cached response

    // Auth header(s) presence
    // Should result in a non-cached response

    // Auth header(s) presence again
    // Should result in a non-cached response

    // Ensure cache is not served to a client with different Auth
    // Ensure auth request with "must-revalidate, s-maxage=0, max-age=0" can be served as staled (section 3.2)

    // Serve stale content if option turned on

    // Cache-Control extensions Not supported yet, will require modifyResponse hook on proxy
}
