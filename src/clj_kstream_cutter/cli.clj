(ns clj-kstream-cutter.cli)

(def cli-options
  ;; An option with a required argument
  [["-b" "--broker comma seperated" "The kafka brokers hosts"
    :validate [#(string? %1) "Must be a sth like HOST:PORT,HOST:PORT"]]
   ["-z" "--zookeeper comma seperated" "The zookeeper hosts"
    :validate [#(string? %1) "Must be a sth like HOST:PORT,HOST:PORT"]]
   ["-i" "--input-topic the topic name" "The input topic name"
    :validate [#(string? %1) "Must be a sth like NAME"]]
   ["-o" "--output-topic the topic name" "The output topic name"
    :validate [#(string? %1) "Must be a sth like NAME"]]
   ["-s" "--selector the .json key selector path" "The .json key selector path"
    :validate [#(not (empty? %1)) "Must be a sth like level1.level2.fieldname"]
    :parse-fn (fn [input-value]
                (list (map #(keyword %1) (clojure.string/split input-value #"\."))))]
   ["-n" "--name the application name" "The application name"
    :validate [#(string? %1) "Must be a sth like NAME"]]
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (clojure.string/join \newline errors)))

(defn usage [options-summary]
  (->> ["This is my program. There are many like it, but this one is mine."
        ""
        "Usage: program-name [options] action"
        ""
        "Options:"
        options-summary]
       (clojure.string/join \newline)))

(defn exit [status _]
  (System/exit status))