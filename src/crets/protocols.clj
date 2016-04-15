(ns crets.protocols)

(defprotocol Interop
  (->clj [this]))

(defprotocol ICommonMetadata
  (classes [this resource-id])
  (lookups [this resource-id]))
