# tcp-pipe

A very simple tcp server and client.

Assumes length prefixed messages serialized with nippy and symmetric key encryption via tempel. Coordination is via clojure.core.async.

Error handling is half baked.

## Usage

```clojure
(require '[com.phronemophobic.tcp-pipe :as tcp-pipe]
         '[clojure.core.async :as async]
         '[taoensso.tempel :as tempel])

;; create a key
(def mykey (tempel/rand-ba 32))

;; start an echo server
(defn echo-handler [socket]
  (tap> (type socket))
  (future
    (with-open [socket socket]
      (let [read-ch (async/chan 10)
            write-ch (async/chan 10)]
        (tcp-pipe/handle-io socket mykey write-ch read-ch)
        (loop []
          (when-let [msg (async/<!! read-ch)]
            (when (async/>!! write-ch msg)
              (recur))))))))

(def server (tcp-pipe/start-server
             5002
             echo-handler))


;; Connect to your echo server
(let [read-ch (async/chan 10)
      write-ch (async/chan 10)]
  (with-open [socket (tcp-pipe/start-client
                      "localhost"
                      5002
                      mykey
                      write-ch
                      read-ch)]
    (async/>!! write-ch "hello")
    (tap> (async/<!! read-ch))
    (async/>!! write-ch "world")
    (tap> (async/<!! read-ch))))
```

## License

Copyright © 2026 Adrian

Licensed under Apache License v2.0.
