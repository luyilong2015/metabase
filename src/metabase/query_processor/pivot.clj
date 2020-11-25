(ns metabase.query-processor.pivot
  "Pivot table actions for the query processor"
  (:require [clojure.set :refer [union]]
            [metabase.util.i18n :refer [trs]]))

;; hey sorry just circled back to this. as an example, let’s take breakouts: [a,b,c,d]

;; those four are split with two as rows and the other two as columns:
;; pivot_rows: [a,b]
;; pivot_cols: [c,d]

;; we need five additional breakout sets:
;; [a,c,d] - subtotal rows
;; [a,b] - “row totals” on the right
;; [a] - subtotal rows within “row totals”
;; [c,d] - “grand totals” row
;; [] - bottom right corner

;; written more generally:
;; [first pivot row + all pivot cols] - subtotal rows
;; [all pivot rows] - “row totals” on the right
;; [first pivot row] - subtotal rows within “row totals”
;; [all pivot cols] - “grand totals” row
;; [] - bottom right corner

;; I think it’s not specced out whether we’d want more sub aggregates if you loaded up like 6 breakouts, 
;; but IMO that probably doesn’t make sense and we could definitely do it later

;; we drop some of these if there are fewer breakouts. e.g. if there’s only one pivot row, we can drop 
;; both subtotal rows and  subtotal rows within "row totals". if there are no pivot_cols, we drop the 
;; row totals on the right

(defn powerset
  "Generate the set of all subsets"
  [items]
  (reduce (fn [s x]
            (union s (map #(conj % x) s)))
          (hash-set #{})
          items))

(defn- generate-breakouts
  "Generate the combinatorial breakouts for a given query pivot table query"
  [breakouts]
  (powerset (set breakouts)))

(defn generate-queries
  "Generate the additional queries to perform a generic pivot table"
  [request]
  (let [query     (:query request)
        breakouts (generate-breakouts (:breakout query))]
    (map (fn [breakout]
           {:breakout breakout
            :query    (assoc query :breakout breakout)}) breakouts)))