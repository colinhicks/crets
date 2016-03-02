(ns crets.query-syntax
  (:require [instaparse.core :as insta]))

;; RETS 1.8.0 Query BNF
(def parser
  (insta/parser
    "
    search-condition ::= query-clause | ( query-clause <ws?> or <ws?> query-clause )
    <query-clause> ::= boolean-element | ( query-clause <ws?> and <ws?> boolean-element )
    <boolean-element> ::= [not] query-element
    <query-element> ::= field-criteria | ( <'('> search-condition <')'> )
    or ::= <'OR'> | <'|'>
    and ::= <'AND'> | <','>
    not ::= <'NOT'> | <'~'>
    <ws> ::= #'\\s'
    field-criteria ::= <'('> field <ws?> <'='> <ws?> field-value <')'>
    <field-value> ::=  period / number / '.EMPTY.' / string-literal / lookup-list / string-list / range-list
    <lookup-list> ::= lookup-or | lookup-not | lookup-and | '.ANY.'
    lookup-or ::= <'|'> lookup ( <','> lookup )*
    lookup-not ::= <'~'> lookup ( <','> lookup )*
    lookup-and ::= <'+'> lookup ( <','> lookup )*
    <lookup> ::= #'[0-9a-zA-Z]+' (* | string-literal *)
    string-list ::= string ( <','> string )*
    <string> ::= string-eq / string-start / string-contains / string-char
    string-eq ::= #'[0-9a-zA-Z]+'
    string-start ::= #'[0-9a-zA-Z]+' <'*'>
    string-contains ::= <'*'> #'[0-9a-zA-Z]+' <'*'>
    string-char ::= ALPHANUM* ('?' ALPHANUM*)*
    string-literal ::= #'[0-9a-zA-Z]+'; (* #'[^\"]+' *( #'[^\"]+' ) *)
    range-list ::= range ( ',' range )*
    range ::= between | greater | less
    between ::= ( period | number ) <'-'> ( period | number )
    greater ::= ( period | number | string-eq ) <'+'>
    less ::= ( period | number | string-eq ) <'-'>
    period ::= (dmqldate | dmqldatetime | partial-time)
    number ::= #'[0-9]+' ; (* ['-']DIGIT ['.' DIGIT*] *)
    <dmqldate> ::= full-date | 'TODAY'
    <dmqldatetime> ::= RETSDATETIME | 'NOW'
    <dmqltime> ::= partial-time
    <DIGIT> = #'[0-9]'
    <ALPHA> = #'[a-zA-Z]'
    <ALPHANUM> = ALPHA | DIGIT
    <RETSDATETIME> ::= date-time | partial-date-time
    field ::= #'[0-9a-zA-Z_]+' ;(* also retsname (ALPHANUM | '_')* *)
    date-fullyear ::= #'[0-9]{4}'
    date-month ::= #'[0-9]{2}'
    date-mday ::= #'[0-9]{2}'
    time-hour ::= #'[0-9]{2}'
    time-minute ::= #'[0-9]{2}'
    time-second ::= #'[0-9]{2}'
    time-secfrac ::= '.'DIGIT
    time-numoffset ::= ('+'|'-') time-hour <':'> time-minute
    time-offset ::= 'Z' | time-numoffset
    <partial-time> ::= time-hour <':'> time-minute <':'> time-second [time-secfrac]
    full-date ::= date-fullyear <'-'> date-month <'-'> date-mday
    full-time ::= partial-time time-offset
    date-time ::= full-date <'T'> full-time
    partial-date-time ::= full-date <'T'> partial-time
    "))

(defn required-criteria [query]
  (->> query
       parser
       (tree-seq vector? rest)
       (filter #(= :field-criteria (first %)))
       (map rest)
       (map (fn [[field & args]] [field [:args args]]))
       (map (partial into {}))))

(defn required-fields [query]
  (map :field (required-criteria query)))

