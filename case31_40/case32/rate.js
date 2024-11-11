import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
    stages: [
        { duration: '60s', target: 5000 },
        { duration: '10s', target: 3000 },
        { duration: '10s', target: 2000 },
        { duration: '10s', target: 1000 },
    ]
};

export default function () {
    const url = 'http://localhost:8080/ratelimit';
    const okStatus = http.expectedStatuses(200)
    // 10% 的请求会带上随机请求头
    let headers = {};
    if (Math.random() < 0.1) {
        headers['vip'] = 'true';
        http.get(url, {
            headers: headers,
            responseCallback: okStatus,
        });
    }else {
        http.get(url, {
            headers: headers
        });
    }

}