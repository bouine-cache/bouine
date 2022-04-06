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
import { sleep } from 'k6';
import { Trend } from 'k6/metrics';
import { getValidJSONBody, headValid, postValid, putValid, patchValid, deleteValid } from './helpers.js'

const waitingTime = new Trend('waitingTime', true);
const baseURL = 'http://bouine1:8080';
// waitingTime.add(res.timings.waiting);

let commonOptions = {
    executor: 'shared-iterations',
    vus: 1,
    iterations: 1,
    startTime: '0s',
    maxDuration: '5s',
    gracefulStop: '5s',
};

export const options = {
    // Storing Responses in Caches : https://datatracker.ietf.org/doc/html/rfc7234#section-3
    scenarios: {
        // HTTP GET test cases
        get200JSONBody:
            // spread operator is not supported in k6 yet (dep on goja)
            // more: https://github.com/grafana/k6/issues/824
            Object.assign({}, commonOptions, {exec: 'get200JSONBody'}),
        get3XXJSONBody: Object.assign({}, commonOptions, {exec: 'get3XXJSONBody'}),
        get404JSONBody: Object.assign({}, commonOptions, {exec: 'get404JSONBody'}),
        get405JSONBody: Object.assign({}, commonOptions, {exec: 'get405JSONBody'}),
        get410JSONBody: Object.assign({}, commonOptions, {exec: 'get410JSONBody'}),
        get414JSONBody: Object.assign({}, commonOptions, {exec: 'get414JSONBody'}),
        get500JSONBody: Object.assign({}, commonOptions, {exec: 'get500JSONBody'}),
        get501JSONBody: Object.assign({}, commonOptions, {exec: 'get501JSONBody'}),
        get502JSONBody: Object.assign({}, commonOptions, {exec: 'get502JSONBody'}),
        get503JSONBody: Object.assign({}, commonOptions, {exec: 'get503JSONBody'}),
        get504JSONBody: Object.assign({}, commonOptions, {exec: 'get504JSONBody'}),
        get200WithSetCookie: Object.assign({}, commonOptions, {exec: 'get200WithSetCookie'}),
        get200WithCacheControlNoStore: Object.assign({}, commonOptions, {exec: 'get200WithCacheControlNoStore'}),
        get200WithCacheControlPrivate: Object.assign({}, commonOptions, {exec: 'get200WithCacheControlPrivate'}),
        get200WithCacheControlPublic: Object.assign({}, commonOptions, {exec: 'get200WithCacheControlPublic'}),
        get200WithBasicAuth: Object.assign({}, commonOptions, {exec: 'get200WithBasicAuth'}),
        get200WithBasicAuthCacheControl: Object.assign({}, commonOptions, {exec: 'get200WithBasicAuthCacheControl'}),
        // Other HTTP verbs test cases
        // FIXME: Broken, seems related to : https://github.com/docker/for-mac/issues/4855
        // head200NoCache: Object.assign({}, commonOptions, {exec: 'head200NoCache'}),
        post200NoCache: Object.assign({}, commonOptions, {exec: 'post200NoCache'}),
        put200NoCache: Object.assign({}, commonOptions, {exec: 'put200NoCache'}),
        patch200NoCache: Object.assign({}, commonOptions, {exec: 'patch200NoCache'}),
        delete200NoCache: Object.assign({}, commonOptions, {exec: 'delete200NoCache'}),
    },
    thresholds: {
        checks: ['rate>0.99'],
        http_req_duration: ['p(90)<2000'], // p90 should be inferior to 1second
    }
}

// This test ensures 200s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get200JSONBody() {
    // First simple GET request on /ernest
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 200, 'rfc7234ValidationGet200JSONBody');

    // Second simple GET request on /ernest
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 200, 'rfc7234ValidationGet200JSONBody');
}

// This test ensures 3XXs JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
// FIXME: The location header is missing from Bouine response when cache hit on both 301 and 302 (Not stored in cache! PR to cache middleware to store Location header)
export function get3XXJSONBody() {
    // First simple GET request on /ernest_301
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 301, 'rfc7234ValidationGet301JSONBody');

    // Second simple GET request on /ernest_301
    // Should result in a cached JSON response
    // getValidJSONBody(baseURL, false, 301, 'rfc7234ValidationGet301JSONBody');

    sleep(Math.random() * 0.15);

    // First simple GET request on /ernest_302
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 302, 'rfc7234ValidationGet302JSONBody');

    // Second simple GET request on /ernest_302
    // Should result in a cached JSON response
    // getValidJSONBody(baseURL, false, 302, 'rfc7234ValidationGet302JSONBody');
}

// This test ensures 404s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get404JSONBody() {
    // First simple GET request on /ernest_404
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 404, 'rfc7234ValidationGet404JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_404
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 404, 'rfc7234ValidationGet404JSONBody');
}

// This test ensures 405s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get405JSONBody() {
    // First simple GET request on /ernest_405
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 405, 'rfc7234ValidationGet405JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_405
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 405, 'rfc7234ValidationGet405JSONBody');
}

// This test ensures 410s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get410JSONBody() {
    // First simple GET request on /ernest_410
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 410, 'rfc7234ValidationGet404JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_410
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 410, 'rfc7234ValidationGet404JSONBody');
}

// This test ensures 414s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get414JSONBody() {
    // First simple GET request on /ernest_414
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 414, 'rfc7234ValidationGet404JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_414
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 414, 'rfc7234ValidationGet404JSONBody');
}

// This test ensures 500s JSON responses are not cached
export function get500JSONBody() {
    // First simple GET request on /ernest_500
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 500, 'rfc7234ValidationGet500JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_500
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 500, 'rfc7234ValidationGet500JSONBody');
}

// This test ensures 501s JSON responses are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7231#section-6.1
export function get501JSONBody() {
    // First simple GET request on /ernest_501
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 501, 'rfc7234ValidationGet500JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_501
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 501, 'rfc7234ValidationGet500JSONBody');
}

// This test ensures 502s JSON responses are not cached
export function get502JSONBody() {
    // First simple GET request on /ernest_502
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 502, 'rfc7234ValidationGet502JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_502
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 502, 'rfc7234ValidationGet502JSONBody');
}

// This test ensures 503s JSON responses are not cached
export function get503JSONBody() {
    // First simple GET request on /ernest_503
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 503, 'rfc7234ValidationGet503JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_503
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 503, 'rfc7234ValidationGet503JSONBody');
}

// This test ensures 504s JSON responses are not cached
export function get504JSONBody() {
    // First simple GET request on /ernest_504
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 504, 'rfc7234ValidationGet504JSONBody');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_504
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 504, 'rfc7234ValidationGet504JSONBody');
}

// This test ensures 200s JSON responses with Set-Cookie header are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-8
export function get200WithSetCookie() {
    // First simple GET request on /ernest_cookie
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 200, 'rfc7234ValidationGet200WithSetCookie', '_cookie');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_cookie
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 200, 'rfc7234ValidationGet200WithSetCookie', '_cookie');
}

// This test ensures HEAD responses are cached
export function head200NoCache() {
    // First simple HEAD request on /ernest_cookie
    // Should result in non-cached JSON response
    headValid(baseURL, false, 200, 'rfc7234ValidationHead200');

    sleep(Math.random() * 0.15);

    // Second simple HEAD request on /ernest_cookie
    // Should result in a cached JSON response
    headValid(baseURL, true, 200, 'rfc7234ValidationHead200');
}

// This test ensures POST responses are not cached
export function post200NoCache() {
    // First simple POST request on /ernest_cookie
    // Should result in non-cached JSON response
    postValid(baseURL, false, 201, 'rfc7234ValidationPost201');

    sleep(Math.random() * 0.15);

    // Second simple POST request on /ernest_cookie
    // Should result in a non-cached JSON response
    postValid(baseURL, false, 201, 'rfc7234ValidationPost201');
}

// This test ensures PATCH responses are not cached
export function patch200NoCache() {
    // First simple PATCH request on /ernest_cookie
    // Should result in non-cached JSON response
    patchValid(baseURL, false, 200, 'rfc7234ValidationPatch200');

    sleep(Math.random() * 0.15);

    // Second simple PATCH request on /ernest_cookie
    // Should result in a non-cached JSON response
    patchValid(baseURL, false, 200, 'rfc7234ValidationPatch200');
}

// This test ensures PUT responses are not cached
export function put200NoCache() {
    // First simple PUT request on /ernest_cookie
    // Should result in non-cached JSON response
    putValid(baseURL, false, 200, 'rfc7234ValidationPut200');

    sleep(Math.random() * 0.15);

    // Second simple PUT request on /ernest_cookie
    // Should result in a non-cached JSON response
    putValid(baseURL, false, 200, 'rfc7234ValidationPut200');
}

// This test ensures DELETE responses are not cached
export function delete200NoCache() {
    // First simple DELETE request on /ernest_cookie
    // Should result in non-cached JSON response
    deleteValid(baseURL, false, 200, 'rfc7234ValidationDelete200');

    sleep(Math.random() * 0.15);

    // Second simple DELETE request on /ernest_cookie
    // Should result in a non-cached JSON response
    deleteValid(baseURL, false, 200, 'rfc7234ValidationDelete200');
}

// This test ensures 200s JSON responses with 'Cache-Control: no-store' header are not cached
// not cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-5.2.2.3
export function get200WithCacheControlNoStore() {
    // First simple GET request on /ernest_no_store
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 200, 'get200WithCacheControlNoStore', '_no_store');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_no_store
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 200, 'get200WithCacheControlNoStore', '_no_store');
}

// This test ensures 200s JSON responses with 'Cache-Control: private' header are not cached
// not cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-5.2.2.6
export function get200WithCacheControlPrivate() {
    // First simple GET request on /ernest_private
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 200, 'get200WithCacheControlPrivate', '_private');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_private
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, false, 200, 'get200WithCacheControlPrivate', '_private');
}

// This test ensures 200s JSON responses with 'Cache-Control: public' header are cached
// cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-5.2.2.5
export function get200WithCacheControlPublic() {
    // First simple GET request on /ernest_public
    // Should result in non-cached JSON response
    getValidJSONBody(baseURL, false, 200, 'get200WithCacheControlPublic', '_public');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_public
    // Should result in a cached JSON response
    getValidJSONBody(baseURL, true, 200, 'get200WithCacheControlPublic', '_public');
}

// This test ensures 200s JSON responses without Cache-Control directive header are not cached
// not cacheable by default as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-3
export function get200WithBasicAuth() {
    // First simple GET request on /ernest_basic_auth_uncached
    // Should result in non-cached JSON response
    // note: Resulting Authorization header = Authorization: Basic YWxhZGRpbjpvcGVuc2VzYW1l
    getValidJSONBody('http://aladdin:opensesame@bouine1:8080', false, 200, 'get200WithBasicAuth', '_basic_auth_uncached');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_basic_auth_uncached
    // Should result in a non-cached JSON response
    // note: Resulting Authorization header = Authorization: Basic YWxhZGRpbjpvcGVuc2VzYW1l
    getValidJSONBody('http://aladdin:opensesame@bouine1:8080', false, 200, 'get200WithBasicAuth', '_basic_auth_uncached');
}

// This test ensures 200s JSON responses with Cache-Control directive header are cached
// cacheable with "must-revalidate, public, and s-maxage" Cache-Control directives
// as specified in https://datatracker.ietf.org/doc/html/rfc7234#section-3.2
export function get200WithBasicAuthCacheControl() {
    // First simple GET request on /ernest_get_200_basic_auth_cached
    // Should result in non-cached JSON response
    // note: Resulting Authorization header = Authorization: Basic YWxhZGRpbjpvcGVuc2VzYW1l
    getValidJSONBody('http://aladdin:opensesame@bouine1:8080', false, 200, 'get200WithBasicAuthCacheControl', '_basic_auth_cached');

    sleep(Math.random() * 0.15);

    // Second simple GET request on /ernest_get_200_basic_auth_cached
    // Should result in a cached JSON response
    // note: Resulting Authorization header = Authorization: Basic YWxhZGRpbjpvcGVuc2VzYW1l
    getValidJSONBody('http://aladdin:opensesame@bouine1:8080', true, 200, 'get200WithBasicAuthCacheControl', '_basic_auth_cached');
}
