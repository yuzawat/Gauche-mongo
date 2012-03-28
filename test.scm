;;; -*- coding: utf-8; mode: scheme -*-
;;;
;;; Test mongo
;;;
(use gauche.test)

(use gauche.uvector)
(use srfi-11)
(debug-print-width #f)

(test-start "mongo")
(test-section "mongo.bson")
(use mongo.bson)
(test-module 'mongo.bson)

(test* "bson-length" #t
       (and 
	(eq? (bson-length #u8()) #f)
	(eq? (bson-length #u8(0 0 0)) #f)
	(equal? (bson-length #u8(0 0 0 0)) 0)
	(equal? (bson-length #u8(1 0 0 0)) 1)
	(equal? (bson-length #u8(255 0 0 0)) 255)
	(equal? (bson-length #u8(255 255 0 0)) 65535)
	(equal? (bson-length #u8(255 255 255 0)) 16777215)
	(equal? (bson-length #u8(255 255 255 127)) 2147483647)
	(equal? (bson-length #u8(0 255 255 128)) -2130706688)))

(test* "string->bson-str " #t
       (let ((str0 "db.example")
	     (bin0 #u8(11 0 0 0 100 98 46 101 120 97 109 112 108 101 0))
	     (str1 "マルチバイト文字列")
	     (bin1 #u8(28 0 0 0 227 131 158 227 131 171 227 131 129 227 131 144 227 130 164 227 131 136 230 150 135 229 173 151 229 136 151 0)))
	 (and
	  (equal? (string->bson-str str0) bin0)
	  (equal? (string->bson-str str1) bin1))))

(test* "bson-str->string " #t
       (let ((str0 "db.example")
	     (bin0 #u8(11 0 0 0 100 98 46 101 120 97 109 112 108 101 0))
	     (str1 "マルチバイト文字列")
	     (bin1 #u8(28 0 0 0 227 131 158 227 131 171 227 131 129 227 131 144 227 130 164 227 131 136 230 150 135 229 173 151 229 136 151 0)))
	 (and
	  (equal? (bson-str->string bin0) str0)
	  (equal? (bson-str->string bin1) str1))))

(test* "string->bson-cstr " #t
       (let ((str0 "db.example")
	     (bin0 #u8(100 98 46 101 120 97 109 112 108 101 0))
	     (str1 "マルチバイト文字列")
	     (bin1 #u8(227 131 158 227 131 171 227 131 129 227 131 144 227 130 164 227 131 136 230 150 135 229 173 151 229 136 151 0)))
	 (and
	  (equal? (string->bson-cstr str0) bin0)
	  (equal? (string->bson-cstr str1) bin1))))

(test* "bson-cstr->string " #t
       (let ((str0 "db.example")
	     (bin0 #u8(100 98 46 101 120 97 109 112 108 101 0))
	     (str1 "マルチバイト文字列")
	     (bin1 #u8(227 131 158 227 131 171 227 131 129 227 131 144 227 130 164 227 131 136 230 150 135 229 173 151 229 136 151 0)))
	 (and
	  (equal? (bson-cstr->string bin0) str0)
	  (equal? (bson-cstr->string bin1) str1))))

(test* "get-ename" #t
       (let1 en (u8vector 1 101 120 97 109 112 108 101 0 122)
	 (let-values (((etype name idx) (get-ename en)))
	   (and (equal? etype 1)
		(equal? name "example")
		(equal? idx 9)))))

(test* "vector->bson-array" #t
       (let ((vec #("a" "b" "c" "d" "e"))
	     (arr '(("0" . "a") ("1" . "b") ("2" . "c") ("3" . "d") ("4" . "e"))))
	 (equal? (vector->bson-array vec) arr)))

(test* "bson-array->vector" #t
       (let ((vec #("a" "b" "c" "d" "e"))
	     (arr '(("0" . "a") ("1" . "b") ("2" . "c") ("3" . "d") ("4" . "e"))))
	 (equal? (bson-array->vector arr) vec)))

(test* "get-binary" #t
       (let-values (((subtype bin) (get-binary (u8vector 16 0 0 0 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 8 97 0 0))))
	 (and
	  (eq? (subtype->symbol subtype) 'bson-bin:generic)
	  (equal? bin (u8vector 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)))))

(test* "get-regexp-str" #t
       (receive (regxp flg end-idx)
	   (get-regexp-str (u8vector 47 94 46 43 42 36 47 0 105 0 106 0 0))
	 (and
	  (equal? regxp "/^.+*$/")
	  (equal? flg "i")
	  (eqv? end-idx 9))))

(test* "get-code_w_s" #t
       (let1 vec (u8vector 57 0 0 0 12 0 0 0 123 114 101 116 117 114 110 32 48 59 125 0 41 0 0 0 1 101 120 97 109 112 108 101 48 0 0 0 0 0 0 0 0 0 1 101 120 97 109 112 108 101 49 0 0 0 0 0 0 0 240 63 0)
	 (receive (code doc) (get-code_w_s vec)
	   (and
	    (equal? code "{return 0;}")
	    (equal? doc '(("example0" . 0.0) ("example1" . 1.0)))))))

(test* "read-element float" #t
       (let ((vec #u8(1 101 120 97 109 112 108 101 0 231 198 244 132 69 74 147 64 8 97 0 0))
	     (val '("example" . 1234.56789)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element utf8-string" #t
       (let ((vec (u8vector 2 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0 8 97 0 0))
	     (val '("example" . "instance")))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element embeded document" #t
       (let ((vec (u8vector 3 101 120 97 109 112 108 101 0 75 0 0 0 1 116 101 115 116 48 0 0 0 0 0 0 0 0 0 1 116 101 115 116 49 0 0 0 0 0 0 0 240 63 2 116 101 115 116 50 0 2 0 0 0 50 0 3 116 101 115 116 51 0 20 0 0 0 1 116 101 115 116 52 0 0 0 0 0 0 0 16 64 0 0 8 97 0 0))
	     (val '("example" . (("test0" . 0.0) ("test1" . 1.0) ("test2" . "2") ("test3" . (("test4" . 4.0)))))))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element array" #t
       (let ((vec (u8vector 4 101 120 97 109 112 108 101 0 70 0 0 0 2 48 0 6 0 0 0 116 101 115 116 48 0 2 49 0 6 0 0 0 116 101 115 116 49 0 2 50 0 6 0 0 0 116 101 115 116 50 0 2 51 0 6 0 0 0 116 101 115 116 52 0 2 52 0 6 0 0 0 116 101 115 116 52 0 0 8 97 0 0))
	     (val '("example" . #("test0" "test1" "test2" "test4" "test4"))))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element binary" #t
       (let ((vec (u8vector 5 101 120 97 109 112 108 101 0 16 0 0 0 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 8 97 0 0))
	     (val '("example" . #u8(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16))))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element objectid" #t
       (let ((vec (u8vector 7 101 120 97 109 112 108 101 0 255 255 255 255 255 255 255 255 255 255 255 255 8 97 0 0))
	     (val '("example" . "ffffffffffffffffffffffff")))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element boolean true" #t
       (let ((vec (u8vector 8 101 120 97 109 112 108 101 0 0 8 97 0 0))
	     (val '("example" . #t)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element boolean false" #t
       (let ((vec (u8vector 8 101 120 97 109 112 108 101 0 1 8 97 0 0))
	     (val '("example" . #f)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element utc datetime" #t
       (let ((vec (u8vector 9 101 120 97 109 112 108 101 0 255 255 255 255 255 255 127 0 8 97 0 0))
	     (val '("example" . 36028797018963967)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element null" #t
       (let ((vec (u8vector 10 101 120 97 109 112 108 101 0 8 97 0 0))
	     (val '("example" . ())))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element regexp" #t
       (let ((vec (u8vector 11 101 120 97 109 112 108 101 0 47 94 46 43 42 36 47 0 105 0 8 97 0 0))
	     (val '("example" . "/^.+*$/i")))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element symbol" #t
       (let ((vec (u8vector 14 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0 8 97 0 0))
	     (val '("example" . instance)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element js-code-w-scope" #t
       (let ((vec (u8vector 15 101 120 97 109 112 108 101 0 57 0 0 0 12 0 0 0 123 114 101 116 117 114 110 32 48 59 125 0 41 0 0 0 1 101 120 97 109 112 108 101 48 0 0 0 0 0 0 0 0 0 1 101 120 97 109 112 108 101 49 0 0 0 0 0 0 0 240 63 0 8 97 0 0))
	     (val '("example" . ("{return 0;}" ("example0" . 0.0) ("example1" . 1.0)))))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element int32" #t
       (let ((vec (u8vector 16 101 120 97 109 112 108 101 0 255 255 0 0 8 97 0 0))
	     (val '("example" . 65535)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element timestamp" #t
       (let ((vec (u8vector 17 101 120 97 109 112 108 101 0 0 0 0 0 0 0 0 0 8 97 0 0))
	     (val '("example" . 0)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element int64" #t
       (let ((vec (u8vector 18 101 120 97 109 112 108 101 0 255 255 255 255 255 255 127 0 8 97 0 0))
	     (val '("example" . 36028797018963967)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element min key" #t
       (let ((vec (u8vector 255 101 120 97 109 112 108 101 0 8 97 0 0))
	     (val '("example" . -inf.0)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "read-element max key" #t
       (let ((vec (u8vector 127 101 120 97 109 112 108 101 0 8 97 0 0))
	     (val '("example" . +inf.0)))
	 (receive (element rest) (read-element vec)
	   (and 
	    (equal? (cons (~ element 'name) (~ element 'value)) val)
	    (equal? rest (u8vector 8 97 0 0))))))

(test* "write-element float" #t
       (let* ((vec #u8(1 101 120 97 109 112 108 101 0 231 198 244 132 69 74 147 64))
	      (val0 '("example" 1234.56789))
	      (val1 '("example" 1234.56789 bson:float))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element utf8-string" #t
       (let* ((vec #u8(2 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0))
	      (val0 '("example" "instance"))
	      (val1 '("example" "instance" bson:utf8-string))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element embeded document" #t
       (let* ((vec #u8(3 101 120 97 109 112 108 101 0 75 0 0 0 1 116 101 115 116 48 0 0 0 0 0 0 0 0 0 1 116 101 115 116 49 0 0 0 0 0 0 0 240 63 2 116 101 115 116 50 0 2 0 0 0 50 0 3 116 101 115 116 51 0 20 0 0 0 1 116 101 115 116 52 0 0 0 0 0 0 0 16 64 0 0))
	     (val0 '("example" (("test0" . 0.0) ("test1" . 1.0) ("test2" . "2") ("test3" . (("test4" . 4.0))))))
	     (val1 '("example" (("test0" . 0.0) ("test1" . 1.0) ("test2" . "2") ("test3" . (("test4" . 4.0)))) bson:embedded-doc))
	     (bin0 (apply write-element val0))
	     (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element array" #t
       (let* ((vec #u8(4 101 120 97 109 112 108 101 0 31 0 0 0 2 48 0 6 0 0 0 116 101 115 116 48 0 2 49 0 6 0 0 0 116 101 115 116 49 0 0))
	      (val0 '("example" #("test0" "test1")))
	      (val1 '("example" #("test0" "test1") bson:array))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element boolean true" #t
       (let* ((vec #u8(8 101 120 97 109 112 108 101 0 0))
	     (val0 '("example" #t))
	     (val1 '("example" #t bson:boolean))
	     (bin0 (apply write-element val0))
	     (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element boolean false" #t
       (let* ((vec #u8(8 101 120 97 109 112 108 101 0 1))
	      (val0 '("example" #f))
	      (val1 '("example" #f bson:boolean))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin1 vec))))

(test* "write-element symbol" #t
       (let* ((vec #u8(14 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0))
	     (val0 '("example" instance))
	     (val1 '("example" instance bson:symbol))
	     (bin0 (apply write-element val0))
	     (bin1 (apply write-element val1)))
	 (and (equal? bin0 vec)
	      (equal? bin0 vec))))

(test* "write-element int32" #t
       (let* ((vec0 (u8vector 16 101 120 97 109 112 108 101 0 0 0 0 128))
	      (vec1 (u8vector 16 101 120 97 109 112 108 101 0 0 0 0 0))
	      (vec2 (u8vector 16 101 120 97 109 112 108 101 0 255 255 0 0))
	      (vec3 (u8vector 16 101 120 97 109 112 108 101 0 255 255 255 127))
	      (val0 '("example" -2147483648 bson:int32))
	      (val1 '("example" 0 bson:int32))
	      (val2 '("example" 65535 bson:int32))
	      (val3 '("example" 2147483647 bson:int32))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1))
	      (bin2 (apply write-element val2))
	      (bin3 (apply write-element val3)))
	 (and (equal? bin0 vec0)
	      (equal? bin1 vec1)
	      (equal? bin2 vec2)
	      (equal? bin3 vec3))))

(test* "write-element int64" #t
       (let* ((vec0 (u8vector 18 101 120 97 109 112 108 101 0 0 160 114 78 24 9 0 128))
	      (vec1 (u8vector 18 101 120 97 109 112 108 101 0 255 255 255 127 255 255 255 255))
	      (vec2 (u8vector 18 101 120 97 109 112 108 101 0 0 0 0 128 0 0 0 0))
	      (vec3 (u8vector 18 101 120 97 109 112 108 101 0 255 95 141 177 231 246 255 127))
	      (val0 '("example" -9223362036854775808 bson:int64))
	      (val1 '("example" -2147483649 bson:int64))
	      (val2 '("example" 2147483648 bson:int64))
	      (val3 '("example" 9223362036854775807 bson:int64))
	      (bin0 (apply write-element val0))
	      (bin1 (apply write-element val1))
	      (bin2 (apply write-element val2))
	      (bin3 (apply write-element val3)))
	 (and (equal? bin0 vec0)
	      (equal? bin1 vec1)
	      (equal? bin2 vec2)
	      (equal? bin3 vec3))))

(test* "make-element" #t
       (equal? (u8vector 2 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0)
	       (make-element 2 (string->bson-cstr "example") (string->bson-str "instance"))))

(test* "check-element-type" #t #t)

(test* "read-bson-document" #t #t)
(test* "write-bson-document" #t #t)

(test* "get-bson-element-list" #t
       (let* ((ls0  '(("example0" . 0) ("example1" . 1)))
	      (ls1  '(("example0" 0 bson:float) ("example1" 1 bson:float)))
	      (ls2  `(,(make <bson-element> :name "example0" :value 0 :type 'bson:float) ,(make <bson-element> :name "example1" :value 1 :type 'bson:float)))
	      (ls3  `(("example0" . 0) ("example1" 1 bson:float) ,(make <bson-element> :name "example2" :value 2 :type 'bson:float))))
	 (and
	  (not (find (^a (not (is-a? a <bson-element>))) (get-bson-element-list ls0)))
	  (not (find (^a (not (is-a? a <bson-element>))) (get-bson-element-list ls1)))
	  (not (find (^a (not (is-a? a <bson-element>))) (get-bson-element-list ls2)))
	  (not (find (^a (not (is-a? a <bson-element>))) (get-bson-element-list ls3))))))

#;(map (^a (describe a))
     (get-bson-element-list 
      `(("example0" . 0) 
	("example1" 1 bson:int32) 
	,(make <bson-element> :name "example2" :value 2 :type 'bson:float)
	("example3" ,(u8vector 255 255 255 255) bson:binary bson-bin:generic)
	("example4")
	("example5" . (("example6" . 6) ("example7" . 7))))))

(test* "<bson-element>" #t
       (let1 element (make <bson-element> :name "example" :value "instance")
	 (and 
	  (equal? (~ element 'name) "example")
	  (equal? (~ element 'value) "instance")
	  (eq? (~ element 'type) 'bson:utf8-string)
	  (equal? (element) (u8vector 2 101 120 97 109 112 108 101 0 9 0 0 0 105 110 115 116 97 110 99 101 0)))))

(test* "bin->elist" #t
       (let1 vec (u8vector 16 101 120 97 109 112 108 48 0 0 0 0 0 16 101 120 97 109 112 108 101 49 0 1 0 0 0)
	 (equal?
	  (map (^a (cons (~ a 'name) (~ a 'value))) (bin->elist vec))
	  '(("exampl0" . 0) ("example1" . 1)))))

(test* "elist->bin" #t
       (equal?
	(elist->bin (get-bson-element-list  '(("exampl0" 0 bson:int32) ("example1" 1 bson:int32))))
	(u8vector 16 101 120 97 109 112 108 48 0 0 0 0 0 16 101 120 97 109 112 108 101 49 0 1 0 0 0)))

(define doc0 '(("title" . (("ja" . "熊を放つ") ("en" . "Setting Free the Bears"))) 
	       ("auther" . "John Winslow Irving") 
	       ("pubulished" . "1968")
	       ("translated" . #t)
	       ("translator" . "村上春樹")
	       ("number_of_bear" 1 bson:int32)))
(define doc1 '(("title" . (("ja" . "ウォーターメソッドマン") ("en" . "The Water-Method Man"))) 
	       ("auther" . "John Winslow Irving") 
	       ("pubulished" . "1972") 
	       ("translated" . #t)
	       ("translator" . #("柴田元幸" "岸本佐知子"))
	       ("number_of_bear" 1 bson:int32)))
(define doc2 '(("title" . (("ja" . "158ポンドの結婚") ("en" . "The 158-Pound Marriage")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1974")
	       ("translated" . #t)
	       ("translator" . "斎藤数衛")
	       ("number_of_bear" 1 bson:int32)))
(define doc3 '(("title" . (("ja" . "ガープの世界") ("en". "The World According to Garp"))) 
	       ("auther" . "John Winslow Irving") 
	       ("pubulished" . "1978")
	       ("translated" . #t)
	       ("translator" . "筒井正明")
	       ("number_of_bear" 1 bson:int32)))
(define doc4 '(("title" . (("ja" . "ホテル・ニューハンプシャー") ("en" . "The Hotel New Hampshire")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1981")
	       ("translated" . #t)
	       ("translator" . "中野圭二")
	       ("number_of_bear" 2 bson:int32)))
(define doc5 '(("title" . (("ja" . "サイダーハウス・ルール") ("en" . "The Cider House Rules")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1985")
	       ("translated" . #t)
	       ("translator" . "真野明裕")))
(define doc6 '(("title" . (("ja" . "オウエンのために祈りを")  ("en" . "A Prayer for Owen Meany")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1989")
	       ("translated" . #t)
	       ("translator" . "中野圭二")
	       ("number_of_bear" 0 bson:int32)))
(define doc7 '(("title" . (("ja" . "サーカスの息子") ("en" . "A Son of the Circus")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1994")
	       ("translated" . #t)
	       ("translator" . "岸本佐知子")
	       ("number_of_bear" 1 bson:int32)))
(define doc8 '(("title" . (("ja" . "未亡人の一年") ("en" .  "A Widow for One Year")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "1998")
	       ("translated" . #t)
	       ("translator" . #("都甲幸治" "中川千帆"))
	       ("number_of_bear" 0 bson:int32)))
(define doc9 '(("title" . (("ja" . "第四の手") ("en" . "The Fourth Hand")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "2001")
	       ("translated" . #t)
	       ("translator" . "小川高義")
	       ("number_of_bear" 0 bson:int32)))
(define doc10 '(("title" . (("ja" . "また会う日まで") ("en" . "Until I Find You")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "2005")
	       ("translated" . #t)
	       ("translator" . "小川高義")))
(define doc11 '(("title" . (("ja" . "あの川のほとりで") ("en" . "Last Night in Twisted River")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "2009")
	       ("translated" . #t)
	       ("translator" . "小竹由美子")))
(define doc12 '(("title" . (("en" . "In One Person")))
	       ("auther" . "John Winslow Irving")
	       ("pubulished" . "2012")
	       ("translated" . #f)))

(define bson0 (u8vector 184 0 0 0 3 116 105 116 108 101 0 57 0 0 0 2 106 97 0 13 0 0 0 231 134 138 227 130 146 230 148 190 227 129 164 0 2 101 110 0 23 0 0 0 83 101 116 116 105 110 103 32 70 114 101 101 32 116 104 101 32 66 101 97 114 115 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 54 56 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 230 157 145 228 184 138 230 152 165 230 168 185 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 1 0 0 0 0))
(define bson1 (u8vector 234 0 0 0 3 116 105 116 108 101 0 76 0 0 0 2 106 97 0 34 0 0 0 227 130 166 227 130 169 227 131 188 227 130 191 227 131 188 227 131 161 227 130 189 227 131 131 227 131 137 227 131 158 227 131 179 0 2 101 110 0 21 0 0 0 84 104 101 32 87 97 116 101 114 45 77 101 116 104 111 100 32 77 97 110 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 55 50 0 8 116 114 97 110 115 108 97 116 101 100 0 0 4 116 114 97 110 115 108 97 116 111 114 0 48 0 0 0 2 48 0 13 0 0 0 230 159 180 231 148 176 229 133 131 229 185 184 0 2 49 0 16 0 0 0 229 178 184 230 156 172 228 189 144 231 159 165 229 173 144 0 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 1 0 0 0 0))
(define bson2 (u8vector 193 0 0 0 3 116 105 116 108 101 0 66 0 0 0 2 106 97 0 22 0 0 0 49 53 56 227 131 157 227 131 179 227 131 137 227 129 174 231 181 144 229 169 154 0 2 101 110 0 23 0 0 0 84 104 101 32 49 53 56 45 80 111 117 110 100 32 77 97 114 114 105 97 103 101 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 55 52 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 230 150 142 232 151 164 230 149 176 232 161 155 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 1 0 0 0 0))
(define bson3 (u8vector 195 0 0 0 3 116 105 116 108 101 0 68 0 0 0 2 106 97 0 19 0 0 0 227 130 172 227 131 188 227 131 151 227 129 174 228 184 150 231 149 140 0 2 101 110 0 28 0 0 0 84 104 101 32 87 111 114 108 100 32 65 99 99 111 114 100 105 110 103 32 116 111 32 71 97 114 112 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 55 56 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 231 173 146 228 186 149 230 173 163 230 152 142 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 1 0 0 0 0))
(define bson4 (u8vector 212 0 0 0 3 116 105 116 108 101 0 85 0 0 0 2 106 97 0 40 0 0 0 227 131 155 227 131 134 227 131 171 227 131 187 227 131 139 227 131 165 227 131 188 227 131 143 227 131 179 227 131 151 227 130 183 227 131 163 227 131 188 0 2 101 110 0 24 0 0 0 84 104 101 32 72 111 116 101 108 32 78 101 119 32 72 97 109 112 115 104 105 114 101 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 56 49 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 228 184 173 233 135 142 229 156 173 228 186 140 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 2 0 0 0 0))
(define bson5 (u8vector 184 0 0 0 3 116 105 116 108 101 0 77 0 0 0 2 106 97 0 34 0 0 0 227 130 181 227 130 164 227 131 128 227 131 188 227 131 143 227 130 166 227 130 185 227 131 187 227 131 171 227 131 188 227 131 171 0 2 101 110 0 22 0 0 0 84 104 101 32 67 105 100 101 114 32 72 111 117 115 101 32 82 117 108 101 115 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 56 53 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 231 156 159 233 135 142 230 152 142 232 163 149 0 0))
(define bson6 (u8vector 206 0 0 0 3 116 105 116 108 101 0 79 0 0 0 2 106 97 0 34 0 0 0 227 130 170 227 130 166 227 130 168 227 131 179 227 129 174 227 129 159 227 130 129 227 129 171 231 165 136 227 130 138 227 130 146 0 2 101 110 0 24 0 0 0 65 32 80 114 97 121 101 114 32 102 111 114 32 79 119 101 110 32 77 101 97 110 121 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 56 57 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 228 184 173 233 135 142 229 156 173 228 186 140 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 0 0 0 0 0))
(define bson7 (u8vector 193 0 0 0 3 116 105 116 108 101 0 63 0 0 0 2 106 97 0 22 0 0 0 227 130 181 227 131 188 227 130 171 227 130 185 227 129 174 230 129 175 229 173 144 0 2 101 110 0 20 0 0 0 65 32 83 111 110 32 111 102 32 116 104 101 32 67 105 114 99 117 115 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 57 52 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 16 0 0 0 229 178 184 230 156 172 228 189 144 231 159 165 229 173 144 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 1 0 0 0 0))
(define bson8 (u8vector 216 0 0 0 3 116 105 116 108 101 0 61 0 0 0 2 106 97 0 19 0 0 0 230 156 170 228 186 161 228 186 186 227 129 174 228 184 128 229 185 180 0 2 101 110 0 21 0 0 0 65 32 87 105 100 111 119 32 102 111 114 32 79 110 101 32 89 101 97 114 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 49 57 57 56 0 8 116 114 97 110 115 108 97 116 101 100 0 0 4 116 114 97 110 115 108 97 116 111 114 0 45 0 0 0 2 48 0 13 0 0 0 233 131 189 231 148 178 229 185 184 230 178 187 0 2 49 0 13 0 0 0 228 184 173 229 183 157 229 141 131 229 184 134 0 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 0 0 0 0 0))
(define bson9 (u8vector 177 0 0 0 3 116 105 116 108 101 0 50 0 0 0 2 106 97 0 13 0 0 0 231 172 172 229 155 155 227 129 174 230 137 139 0 2 101 110 0 16 0 0 0 84 104 101 32 70 111 117 114 116 104 32 72 97 110 100 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 50 48 48 49 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 229 176 143 229 183 157 233 171 152 231 190 169 0 16 110 117 109 98 101 114 95 111 102 95 98 101 97 114 0 0 0 0 0 0))
(define bson10 (u8vector 167 0 0 0 3 116 105 116 108 101 0 60 0 0 0 2 106 97 0 22 0 0 0 227 129 190 227 129 159 228 188 154 227 129 134 230 151 165 227 129 190 227 129 167 0 2 101 110 0 17 0 0 0 85 110 116 105 108 32 73 32 70 105 110 100 32 89 111 117 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 50 48 48 53 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 13 0 0 0 229 176 143 229 183 157 233 171 152 231 190 169 0 0))
(define bson11 (u8vector 184 0 0 0 3 116 105 116 108 101 0 74 0 0 0 2 106 97 0 25 0 0 0 227 129 130 227 129 174 229 183 157 227 129 174 227 129 187 227 129 168 227 130 138 227 129 167 0 2 101 110 0 28 0 0 0 76 97 115 116 32 78 105 103 104 116 32 105 110 32 84 119 105 115 116 101 100 32 82 105 118 101 114 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 50 48 48 57 0 8 116 114 97 110 115 108 97 116 101 100 0 0 2 116 114 97 110 115 108 97 116 111 114 0 16 0 0 0 229 176 143 231 171 185 231 148 177 231 190 142 229 173 144 0 0))
(define bson12 (u8vector 105 0 0 0 3 116 105 116 108 101 0 27 0 0 0 2 101 110 0 14 0 0 0 73 110 32 79 110 101 32 80 101 114 115 111 110 0 0 2 97 117 116 104 101 114 0 20 0 0 0 74 111 104 110 32 87 105 110 115 108 111 119 32 73 114 118 105 110 103 0 2 112 117 98 117 108 105 115 104 101 100 0 5 0 0 0 50 48 49 50 0 8 116 114 97 110 115 108 97 116 101 100 0 1 0))


(test* "list->bson" #t
       (and 
	(equal? (list->bson doc0) bson0)
	(equal? (list->bson doc1) bson1)
	(equal? (list->bson doc2) bson2)
	(equal? (list->bson doc3) bson3)
	(equal? (list->bson doc4) bson4)
	(equal? (list->bson doc5) bson5)
	(equal? (list->bson doc6) bson6)
	(equal? (list->bson doc7) bson7)
	(equal? (list->bson doc8) bson8)
	(equal? (list->bson doc9) bson9)
	(equal? (list->bson doc10) bson10)
	(equal? (list->bson doc11) bson11)
	(equal? (list->bson doc12) bson12)))

(test* "bson->list" #t #t)

(test-section "mongo")
(use mongo)
(test-module 'mongo)

(test* "make-requestID" #t
       (let1 id (make-requestID)
	 (and (number? id) (<= -2147483647 id 2147483648))))

(test* "get-responseTo" #t
       (let1 resp (u8vector 90 1 0 0 24 54 129 129 153 241 65 75 1 0 0 0 8 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 54 1 0 0 4 100 97 116 97 98 97 115 101 115 0 7 1 0 0 3 48 0 48 0 0 0 2 110 97 109 101 0 5 0 0 0 116 101 115 116 0 1 115 105 122 101 79 110 68 105 115 107 0 0 0 0 0 0 0 170 65 8 101 109 112 116 121 0 0 0 3 49 0 51 0 0 0 2 110 97 109 101 0 8 0 0 0 101 120 97 109 112 108 101 0 1 115 105 122 101 79 110 68 105 115 107 0 0 0 0 0 0 0 170 65 8 101 109 112 116 121 0 0 0 3 50 0 49 0 0 0 2 110 97 109 101 0 6 0 0 0 97 100 109 105 110 0 1 115 105 122 101 79 110 68 105 115 107 0 0 0 0 0 0 0 240 63 8 101 109 112 116 121 0 1 0 3 51 0 46 0 0 0 2 110 97 109 101 0 3 0 0 0 100 98 0 1 115 105 122 101 79 110 68 105 115 107 0 0 0 0 0 0 0 240 63 8 101 109 112 116 121 0 1 0 3 52 0 49 0 0 0 2 110 97 109 101 0 6 0 0 0 108 111 99 97 108 0 1 115 105 122 101 79 110 68 105 115 107 0 0 0 0 0 0 0 240 63 8 101 109 112 116 121 0 1 0 0 1 116 111 116 97 108 83 105 122 101 0 0 0 0 0 0 0 186 65 1 111 107 0 0 0 0 0 0 0 240 63 0)
	 (= 1262612889 (get-responseTo resp))))

(test* "check-element-name" #t
       (and
	(check-element-name "test")
	(not (check-element-name "test.test"))
	(not (check-element-name "$test"))
	(not (check-element-name "_id"))))

(define mongo #f)

(test* "<mongo>" #t
       (guard (exc 
	       ((condition-has-type? exc <mongo-error>)
		(print "Can't connect mongodb."))
	       (else
		(print "Something happened.")))
	      (let1 m (make <mongo>)
		(set! mongo m)
		(is-a? mongo <mongo>))))

(if (is-a? mongo <mongo>)
    (begin
      (test* "insert" (undefined)
	     (for-each (^a (insert mongo "test.books" (list a)))
		       `(,doc0 ,doc1 ,doc2 ,doc3 ,doc4 ,doc5 ,doc6 ,doc7 ,doc8 ,doc9 ,doc10 ,doc11 ,doc12)))
      
      (test* "query" #t
	     (let1 titles '("Setting Free the Bears" "The Water-Method Man" "The World According to Garp" "The Hotel New Hampshire" "The 158-Pound Marriage" "The Cider House Rules" "A Prayer for Owen Meany" "A Son of the Circus" "A Widow for One Year" "The Fourth Hand" "Until I Find You" "Last Night in Twisted River" "In One Person")
	       (equal?
		(sort titles)
		(sort (map (^a (cdr (assoc "en" (cdr (assoc "title" a)))))
			   ((query mongo "test.books" 0 0 '())))))))


      (test* "update" (undefined)
	     (update mongo "test.books" 
		     '(("title" . (("ja" . "ウォーターメソッドマン") ("en" . "The Water-Method Man")))) 
		     '(("title" . (("ja" . "水療法の男") ("en" . "The Water-Method Man")))
		       ("auther" . "John Winslow Irving") 
		       ("pubulished" . "1972") 
		       ("translated" . #t)
		       ("translator" . #("柴田元幸" "岸本佐知子"))
		       ("number_of_bear" 1 bson:int32))))

      (test* "update confirm" #t
	     (equal? "水療法の男"
		     (cdr (assoc "ja" 
				 (cdr (assoc "title" (car 
						      ((query mongo "test.books" 0 1 
							      '(("title" . (("ja" . "水療法の男") ("en" . "The Water-Method Man")))))))))))))
      (test* "getmore" #t
	     (let1 mongo-reply (query mongo "test.books" 0 6 '())
	       (and
		(= (length (mongo-reply)) 6)
		(let1 get-more-reply (getmore mongo "test.books" 10 (~ mongo-reply 'cursorID))
		  (= (length (get-more-reply)) 7)))))

      (test* "kill-cusors" #t
	     (let1 mongo-reply (query mongo "test.books" 0 5 '())
	       (let1 cursorID (~ mongo-reply 'cursorID)
		 (and
		  (= (length ((getmore mongo "test.books" 2 cursorID))) 2)
		  (begin
		    (kill-cusors mongo `(,cursorID))
		    (= (length ((getmore mongo "test.books" 2 cursorID))) 0))))))
    
      (test* "delete" (undefined)
	     (delete (make <mongo>) "test.books" '()))

      (test* "delete confirm" #t
	     (if (null? ((query mongo "test.books" 0 0 '()))) #t))))

;; epilogue
(test-end)
