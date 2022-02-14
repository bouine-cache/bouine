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
import http from 'k6/http';
import { check } from 'k6';

export function getValidJSONBody(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.get(`${baseURL}/ernest_get_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}

export function headValid(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.head(`${baseURL}/ernest_head_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}

export function postValid(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.post(`${baseURL}/ernest_post_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}

export function deleteValid(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.delete(`${baseURL}/ernest_delete_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}

export function putValid(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.put(`${baseURL}/ernest_put_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}

export function patchValid(baseURL, cached, expectedStatus, testName, customSuffix='') {
    const res = http.patch(`${baseURL}/ernest_patch_${expectedStatus}${customSuffix}`, { redirects: 0 });

    check(res, {
        [`${testName} - ${expectedStatus} status is expected`]: (r) => r.status === expectedStatus,
    });

    if (cached) {
        check(res, {
            [`${testName} - X-Cache must be equal to 'hit'`]: (r) => r.headers['X-Cache'] === 'hit',
        });
        return;
    }

    check(res, {
        [`${testName} - X-Cache must NOT be equal to 'hit'`]: (r) => r.headers['X-Cache'] !== 'hit',
    });
}
