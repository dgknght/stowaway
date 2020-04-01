# stowaway

A Clojure library designed to abstract the storage mechanism from the business logic.

## Usage

Within the application, when access to the data store is needed,
wrap the usage in `with-storage`, which reifies a storage stragey.
```clojure
(:require [stowaway.core :as storage])

(with-storage [storage config]
  (storage/create my-model))
```

## Configuration
One or more strategies can be registered to be used at runtime. One way to manage
storing different models is to create multi methods for each type of model.

```clojure
(:require [stowaway.core :as storage :refer [Storage]])

; Define the multi methods for handling CRUD actions
(defn- dispatch-model
  [model & _]
  (storage/tag model))

(defmulti select dispatch-model)
(defmulti create dispatch-model)
(defmulti update dispatch-model)
(defmulti delete dispatch-model)

; Implement the protocol
(MyStorage [config]
  Storage
  (select [_ criteria options] (select criteria options config))
  (create [_ model] (create model config))
  (update [_ model] (update model config))
  (delete [_ model] (create model config)))

; Implement the functions to perfrom the storage actions
; specific to this storage strategy
(defmethod create ::models/user
  [user config]
  (create-user-record-some-specific-way user config))

(defmethod create ::models/address
  [address config]
  (create-address-record-some-specific-way address config))

;... and all the others

; Register this strategy
(storage/register-strategy
  (fn [config]
    (when (can-use-this-config? config)
      (MyStorage. config))))
```

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
