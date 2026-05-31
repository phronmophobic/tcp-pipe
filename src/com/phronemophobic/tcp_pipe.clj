(ns com.phronemophobic.tcp-pipe
  (:require[clojure.core.async :as async]
           [taoensso.nippy :as nippy]
           [taoensso.tempel :as tempel])
  (:import [java.net InetSocketAddress Socket StandardSocketOptions ServerSocket]
           java.io.DataOutputStream
           java.io.EOFException
           java.net.SocketException
           java.io.DataInputStream))

(defn handle-io [socket key write-ch read-ch]
  (let [is (.getInputStream socket)
        os (.getOutputStream socket)]
    (future
      (try
        (with-open [socket socket
                    ^java.io.OutputStream
                    os os
                    ^java.io.InputStream
                    is is
                    is (DataInputStream. is)]
          (loop []
            (let [num-bytes (.readInt is)
                  bs (byte-array num-bytes)]
              (.readFully is bs)
              (when (async/>!! read-ch
                               (-> bs
                                   (tempel/decrypt-with-symmetric-key key)
                                   nippy/thaw))
                (recur)))))
        (catch EOFException e
          (async/close! write-ch)
          nil)
        (catch SocketException e
          (async/close! write-ch)
          nil)
        (catch Throwable t
          (prn t)
          (tap> t)
          (throw t))
        (finally
          (async/close! read-ch))))

    (future
      (try
        (with-open [socket socket
                    ^java.io.OutputStream
                    os os
                    ^java.io.InputStream
                    is is
                    os (DataOutputStream. os)]
          (loop []
            (when-let [val (async/<!! write-ch)]
              (let [bs (-> val
                           nippy/freeze
                           (tempel/encrypt-with-symmetric-key key))]
                (.writeInt os (alength bs))
                (.write os bs))
              (recur))))
        (catch EOFException e
          (async/close! read-ch)
          nil)
        (catch SocketException e
          (async/close! read-ch)
          nil)
        (catch Throwable t
          (prn t)
          (tap> t)
          (throw t))
        (finally
          (async/close! write-ch))))))



(defn start-server [port handler]
  (let [serverSocket ( ServerSocket. port)]
    (future
      (try
        (loop []
          (let [socket (.accept serverSocket)]
            (handler socket)
            (recur)))
        (finally
          (prn "stopping server"))))
    serverSocket))

(defn start-client [host port key write-ch read-ch]
  (let [socket (Socket. host port)]
    (handle-io socket key write-ch read-ch)
    socket))


