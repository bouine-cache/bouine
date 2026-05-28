vcl 4.1;

import std;

backend origin {
    .host = "origin";
    .port = "8080";
    .connect_timeout = 5s;
    .first_byte_timeout = 30s;
    .between_bytes_timeout = 5s;
    .max_connections = 1000;
}

sub vcl_recv {
    # Normalize host
    set req.http.Host = "origin:8080";
    # Strip cookies so responses are cached
    unset req.http.Cookie;
    return(hash);
}

sub vcl_hash {
    hash_data(req.url);
    hash_data(req.http.host);
    return(lookup);
}

sub vcl_backend_response {
    # Use Cache-Control from origin; set a floor of 60s for responses
    # without explicit TTL
    if (beresp.ttl < 60s && beresp.status == 200) {
        if (!beresp.http.Cache-Control) {
            set beresp.ttl = 60s;
        }
    }
    # Enable stale-while-revalidate
    set beresp.grace = 30s;
    return(deliver);
}

sub vcl_deliver {
    if (obj.hits > 0) {
        set resp.http.X-Cache = "HIT";
    } else {
        set resp.http.X-Cache = "MISS";
    }
}
