;;; -*- coding: utf-8; mode: scheme -*-
;;
;; BSON parser & constructer
;;
;; (bson->list bson-document) => list
;;   bson-document as u8vector
;;   list as '(("key0" . val0) ("key1" . val1) ("key2" . val2))
;;
;; (list->bson list) => bson-document
;;   bson-document as u8vector
;;   list as '(("key0" . val0) ("key1" . val1) ("key2" . val2))
;;                     or
;;           '(("key0" val0 type0) ("key1" val1 type1) ("key2" val2 type2))
;;                     or
;;           '(<bson-element> <bson-element> <bson-element>)
;;
;; (read-bson-document bson-document) => <bson>
;; (write-bson-document list) => <bson>
;; (make <bson-element> :name name :value value :type type) => <bson-element>
;; (<bson-element>) => bson-element 
;;    bson-element as u8vector
;;
;;  * BSON spec: <http://bsonspec.org/#/specification>

(define-module mongo.bson
  (use binary.io)
  (use gauche.array)
  (use gauche.sequence)
  (use gauche.uvector)
  (use srfi-1)
  (use srfi-11)
  (use util.match)
  (export
   bson->list
   list->bson
   <bson>
   <bson-element>

   bin->elist
   elist->bin
   read-element
   write-element

   subtype->symbol
   make-element
   get-bson-element-list
   get-ename
   bson-length
   bson-str->string
   bson-cstr->string
   string->bson-str
   string->bson-cstr
   vector->bson-array
   bson-array->vector
   get-binary
   get-regexp-str
   get-code_w_s
   ))

(select-module mongo.bson)

(define-condition-type <bson-error> <error>
  bson-error?
  (reason bson-error-reason))

(define-class <bson> ()
  ((size :allocation :instance)
   (elist :allocation :instance
	  :init-keyword :elist)
   (doc :allocation :instance
	:init-keyword :doc)))

(define-method initialize ((bson <bson>) initargs)
  (next-method))

(define-method object-apply ((bson <bson>))
  (let* ((elist (elist->bin (~ bson 'elist)))
	 (buff (make-u8vector (+ Length-head (uvector-length elist) 1))))
    (put-s32le! buff 0 (uvector-length buff))
    (u8vector-copy! buff 4 elist)
    buff))

(define-method pop! ((bson <bson>))
  (undefined))

(define-method push! ((bson <bson>))
  (undefined))

(define-class <bson-element> ()
  ((type :allocation :instance
	 :init-keyword :type)
   (name :allocation :instance
	 :init-keyword :name)
   (value :allocation :instance
	  :init-keyword :value)
   (subtype :allocation :instance
	    :init-keyword :subtype)))

(define-method initialize ((element <bson-element>) initargs)
  (next-method)
  (unless (slot-bound? element 'type)
    (set! (~ element 'type) 
	  (etype->symbol 
	   (check-element-type (~ element 'value))))))

(define-method object-apply ((element <bson-element>))
  (write-element (~ element 'name) 
		 (~ element 'value)
		 (~ element 'type)))

(define-constant Zero 0)
(define-constant Length-head 4)

;; utils
(define (in one list)
  (if (find (^a (eq? a one)) list) #t
      #f))

(define (flatten ls)
  (cond ((null? ls) '())
        ((pair? ls) (append (flatten (car ls)) (flatten (cdr ls))))
        (else (list ls))))

(define (lister ls . args)
  (append ls (flatten args)))

(define (atom? a)
  (if (pair? a) #f #t))

;; read bson
(define (bson->list document)
  (let1 bson (read-bson-document document)
    (map (^a (cons (~ a 'name) (~ a 'value))) (~ bson 'elist))))

(define (read-bson-document document)
  (let* ((len (uvector-length document))
	 (elist-len (bson-length document))
	 (elist (u8vector-copy document Length-head (- elist-len 1))))
    (make <bson> :elist (bin->elist elist))))

(define (bin->elist elist)
  (let loop ((elements elist)
	     (ls '()))
    (let-values (((element rest) (read-element elements)))
      (if (= (uvector-length rest) 0) (lister ls element)
	  (loop rest (lister ls element))))))

(define (read-element elements)
  (let-values 
      (((etype ename val-index) (get-ename elements)))
    (case etype
      ([1]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value (get-f64 elements val-index 'little-endian))
	(u8vector-copy elements (+ val-index 8))))
      ([2]
       (let1 str (u8vector-copy elements val-index)
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (bson-str->string str))
	  (u8vector-copy elements (+ val-index (bson-length str) Length-head)))))
      ([3]
       (let1 doc (u8vector-copy elements val-index)
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (bson->list doc))
	  (u8vector-copy elements (+ val-index (bson-length doc))))))
      ([4]
       (let1 doc (u8vector-copy elements val-index)
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (bson-array->vector (bson->list doc)))
	  (u8vector-copy elements (+ val-index (bson-length doc))))))
      ([5]
       (let1 bin (u8vector-copy elements val-index)
	 (values
	  (let-values (((subtype binary) (get-binary (u8vector-copy bin))))
	    (make <bson-element> :type (etype->symbol etype) :name ename 
		:value binary :subtype (subtype->symbol subtype)))
	  (u8vector-copy elements (+ val-index (bson-length bin) Length-head 1)))))
      ([7]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
              :value (fold-right string-append ""
                                 (map (^a (format #f "~2,'0x" a))
                                      (u8vector->list
                                       (u8vector-copy elements val-index (+ val-index 12))))))
	(u8vector-copy elements (+ val-index 12))))
      ([8]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename
	      :value (if (= (get-u8 elements val-index) 0) #t #f))
	(u8vector-copy elements (+ val-index 1))))
      ([9]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value (get-s64 elements val-index 'little-endian))
	(u8vector-copy elements (+ val-index 8))))
      ([10]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value '())
	(u8vector-copy elements val-index)))
      ([11]
       (let-values (((regexp flg end-idx) (get-regexp-str (u8vector-copy elements val-index))))
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (string-append regexp flg))
	  (u8vector-copy elements (+ val-index end-idx 1)))))
      ([13]
       (let1 str (u8vector-copy elements val-index)
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (bson-str->string str))
	  (u8vector-copy elements (+ val-index (bson-length str) Length-head)))))
      ([14]
       (let1 str (u8vector-copy elements val-index)
	 (values
	  (make <bson-element> :type (etype->symbol etype) :name ename 
		:value (string->symbol (bson-str->string str)))
	  (u8vector-copy elements (+ val-index (bson-length str) Length-head)))))
      ([15]
       (let1 str (u8vector-copy elements val-index)
	 (let-values (((code doc) (get-code_w_s str)))
	   (values
	    (make <bson-element> :type (etype->symbol etype) :name ename 
		  :value `(,code . ,doc))
	    (u8vector-copy elements (+ val-index (bson-length str) Length-head))))))
      ([16]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value (get-s32 elements val-index 'little-endian))
	(u8vector-copy elements (+ val-index 4))))
      ([17]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value (get-s64 elements val-index 'little-endian))
	(u8vector-copy elements (+ val-index 8))))
      ([18]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value (get-s64 elements val-index 'little-endian))
	(u8vector-copy elements (+ val-index 8))))
      ([255]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value -inf.0)
	(u8vector-copy elements val-index)))
      ([127]
       (values
	(make <bson-element> :type (etype->symbol etype) :name ename 
	      :value +inf.0)
	(u8vector-copy elements val-index)))
      (else (error <bson-error> "No such element type")))))

;; write bson
(define (list->bson ls)
  (let1 bson (make <bson> :elist (get-bson-element-list ls)) 
    (let* ((elist-bin (elist->bin (~ bson 'elist)))
	   (buff (make-u8vector (+ Length-head (uvector-length elist-bin) 1))))
      (put-s32le! buff 0 (uvector-length buff))
      (u8vector-copy! buff 4 elist-bin)
      buff)))

(define (get-bson-element-list ls)
  (map (^e (match e
		  (((? string? a) (? atom? b) (? symbol? c) (? symbol? d)) 
		   (make <bson-element> :name a :value b :type c :subtype d))
		  (((? string? a) (? atom? b) (? symbol? c)) 
		   (make <bson-element> :name a :value b :type c))
		  (((? string? a) . (? null? b)) 
		   (make <bson-element> :name a :value +nan.0))
		  (((? string? a) . (? list? b)) 
		   (make <bson-element> :name a :value b))
		  (((? string? a) . (? atom? b)) 
		   (make <bson-element> :name a :value b))
		  (($ <bson-element>) e)
		  (else (error <bson-error> "list required")))) ls))

(define (elist->bin ls)
  (let* ((vec-ls (map (^a (a)) ls))
	 (buff (make-u8vector (fold + 0 (map (^a (uvector-length a)) vec-ls)))))
    (let loop ((idx 0)
	       (vec vec-ls))
      (if (= idx (uvector-length buff)) #f
	  (begin
	    (u8vector-copy! buff idx (car vec))
	    (loop (+ idx (uvector-length (car vec))) (cdr vec)))))
    buff))

(define (check-element-type eval)
  (cond ((eq? eval -inf.0) 255)
	((eq? eval +inf.0) 127)
	((eq? eval +nan.0) 10)
	((is-a? eval <real>) 1)
	((is-a? eval <string>) 2)
	((is-a? eval <list>) 3)
	((is-a? eval <vector>) 4)
	((is-a? eval <u8vector>) 5)
	((is-a? eval <boolean>) 8)
	((is-a? eval <time>) 9)
	((is-a? eval <null>) 10)
	((is-a? eval <regexp>) 11)
	((is-a? eval <symbol>) 14)
	((is-a? eval <integer>) 
	 (if (<= -9223372036854775808 eval 9223362036854775807)
	     (if (<= -2147483648 eval 2147483647) 16 18)
	     (error <bson-error> "Too big integer")))
	(else (error <bson-error> "Can't datermin element type"))))

(define (write-element ename eval . etype-sym)
  (let ((ename-vec (string->bson-cstr ename))
	(etype (if (and (pair? etype-sym) (car etype-sym)) 
		   (symbol->etype (car etype-sym))
		   (check-element-type eval))))
    (case etype
      ([1]
       (make-element etype ename-vec 
		     (let1 buff (make-u8vector 8) (put-f64le! buff 0 eval) buff)))
      ([2]
       (make-element etype ename-vec (string->bson-str eval)))
      ([3]
       (make-element etype ename-vec (list->bson eval)))
      ([4]
       (make-element etype ename-vec (list->bson (vector->bson-array eval))))
      ([5]
       (make-element etype ename-vec eval))
      ([6]
       (make-element etype ename-vec eval))
      ([8]
       (make-element etype ename-vec (make-u8vector 1 (if (boolean eval) 0 1))))
      ([9]
       (make-element etype ename-vec eval))
      ([10]
       (make-element etype ename-vec))
      ([11]
       (make-element etype ename-vec eval))
      ([12]
       (make-element etype ename-vec (string->bson-str eval)))
      ([14]
       (make-element etype ename-vec (string->bson-str (symbol->string eval))))
      ([15]
       (make-element etype ename-vec (string->bson-str eval)))
      ([16]
       (make-element etype ename-vec 
		     (let1 buff (make-u8vector 4) (put-s32le! buff 0 eval) buff)))
      ([18]
       (make-element etype ename-vec 
		     (let1 buff (make-u8vector 8) (put-s64le! buff 0 eval) buff)))
      ([17]
       (make-element etype ename-vec 
		     (let1 buff (make-u8vector 8) (put-s64le! buff 0 eval) buff)))
      ([255]
       (make-element etype ename-vec))
      ([127]
       (make-element etype ename-vec))
      (else (error <bson-error> "Can't datermin element type")))))

(define (make-element etype ename-vec . eval-vec)
  (let* ((eval-v (if (pair? eval-vec) (car eval-vec) #f))
	 (ename-len (uvector-length ename-vec))
	 (buff (make-u8vector (+ 1 ename-len 
				 (if eval-v (uvector-length eval-v) 0)))))
    (u8vector-copy! buff 0 (make-u8vector 1 etype))
    (u8vector-copy! buff 1 ename-vec)
    (if eval-v
	(u8vector-copy! buff (+ 1 ename-len) eval-v))
    buff))

;; tools
(define (etype->symbol etype)
  (case etype
    ([1] 'bson:float)
    ([2] 'bson:utf8-string)
    ([3] 'bson:embedded-doc)
    ([4] 'bson:array)
    ([5] 'bson:binary)
    ([7] 'bson:objectid)
    ([8] 'bson:boolean)
    ([9] 'bson:utc-datetime)
    ([10] 'bson:null)
    ([11] 'bson:regexp)
    ([13] 'bson:js-code)
    ([14] 'bson:symbol)
    ([15] 'bson:js-code-w-scope)
    ([16] 'bson:int32)
    ([17] 'bson:timestamp)
    ([18] 'bson:int64)
    ([255] 'bson:min-key)
    ([127] 'bson:max-key)
    (else (error <bson-error> "Not such element type"))))

(define (symbol->etype sym)
  (case sym
    ([bson:float] 1)
    ([bson:utf8-string] 2)
    ([bson:embedded-doc] 3)
    ([bson:array] 4)
    ([bson:binary] 5)
    ([bson:objectid] 7)
    ([bson:boolean] 8)
    ([bson:utc-datetime] 9)
    ([bson:null] 10)
    ([bson:regexp] 11)
    ([bson:js-code] 13)
    ([bson:symbol] 14)
    ([bson:js-code-w-scope] 15)
    ([bson:int32] 16)
    ([bson:timestamp] 17)
    ([bson:int64] 18)
    ([bson:min-key] 255)
    ([bson:max-key] 127)
    (else (error <bson-error> "Not such element type"))))

(define (subtype->symbol subtype)
  (case subtype
    ([0] 'bson-bin:generic)
    ([1] 'bson-bin:function)
    ([2] 'bson-bin:binary-old)
    ([3] 'bson-bin:uuid)
    ([4] 'bson-bin:md5)
    ([5] 'bson-bin:user-defined)
    (else (error <bson-error> "Not such binary subtype"))))

(define (symbol->subtype sym)
  (case sym
    ([bson-bin:generic] 0)
    ([bson-bin:function] 1)
    ([bson-bin:binary-old] 2)
    ([bson-bin:uuid] 3)
    ([bson-bin:md5] 4)
    ([bson-bin:user-defined] 5)
    (else (error <bson-error> "Not such binary subtype"))))

(define (get-ename element)
  (let1 val-index (find-index (^a (= a Zero)) element)
    (values
     (get-u8 element 0)
     (u8vector->string 
      (subseq element 1 val-index))
     (+ val-index 1))))

(define (bson-length bson)
  (if (>= (uvector-length bson) 4)
      (get-s32 bson 0 'little-endian)
      #f))

(define (string->bson-cstr str)
  (let* ((bcstr (string->u8vector str))
	 (len (uvector-length bcstr))
	 (buff (make-u8vector (+ len 1))))
    (u8vector-set! (u8vector-copy! buff bcstr 0) len Zero)))

(define (bson-cstr->string bcstr)
  (let1 str (subseq bcstr 0 (find-index (^a (= a Zero)) bcstr))
    (u8vector->string (u8vector-copy str 0))))

(define (string->bson-str str)
  (let* ((bstr (string->u8vector str))
	 (len (uvector-length bstr))
	 (head (uvector-alias <u8vector> (s32vector (+ len 1))))
	 (buff (make-u8vector (+ 4 len 1))))
    (u8vector-copy! buff 0 head)
    (u8vector-copy! buff 4 bstr)
    (u8vector-set! buff (+ 4 len) Zero)
    buff))

(define (bson-str->string bstr)
  (let1	len (bson-length bstr)
    (u8vector->string 
     (u8vector-copy bstr Length-head 
		    (- (+ Length-head len) 1)))))

(define (vector->bson-array vec)
  (map (^ (a b) (cons (number->string a) b)) 
       (iota (vector-length vec)) vec))

(define (bson-array->vector barray)
  (list->vector (map (^a (cdr a)) barray)))

(define (get-binary binary)
  (let ((len (bson-length binary)))
    (values
     (get-u8 binary 4)
     (subseq binary 5 (+ 5 len)))))

(define (get-regexp-str str)
  (let* ((len (uvector-length str))
	 (first-idx (find-index (^a (= a Zero)) str))
	 (second-idx (+ (find-index (^a (= a Zero)) (subseq str (+ first-idx 1) (- len 1))) first-idx 1)))
    (values
     (u8vector->string (subseq str 0 first-idx))
     (u8vector->string (subseq str (+ first-idx 1) second-idx))
     second-idx)))

(define (get-code_w_s str)
  (let ((code-len (bson-length (u8vector-copy str Length-head))))
    (values
     (bson-str->string (u8vector-copy str Length-head))
     (bson->list (u8vector-copy str (+ Length-head Length-head code-len))))))

;; Epilogue
(provide "mongo.bson")
