;;; -*- coding: utf-8; mode: scheme -*-
;;;
;;; mongoDB driver for Gauche
;;;
;;;  mongoDB <http://www.mongodb.org/>

(define-module mongo
  (use gauche.uvector)
  (use rfc.json)
  (use gauche.net)
  (use binary.io)
  (use srfi-1)
  (use srfi-27)
  (use mongo.bson)
  (export
   <mongo>
   <mongo-error>
   req->resp
   req-only
   make-requestID
   get-responseTo
   check-element-name
   check-element-names
   write-buffer
   read-buffer
   _update
   _query
   _insert
   _getmore
   _delete
   _kill-cusors))

(select-module mongo)

(define-condition-type <mongo-error> <error>
  mongo-error?
  (reason mongo-error-reason))

(define-class <mongo> ()
  ((host :allocation :instance
         :init-keyword :host
         :init-value "localhost")
   (port :allocation :instance
         :init-keyword :port
         :init-value 27017)
   (in :allocation :instance)
   (out :allocation :instance)
   (sock :allocation :instance)))

(define-method initialize ((mongo <mongo>) initargs)
  (next-method)
  (guard (exc
	  ((condition-has-type? exc <system-error>)
	   (error <mongo-error> 
		  #`"Failed to connect with ,(~ mongo 'host):,(~ mongo 'port)"))
	  (else <mongo-error> "Something happened"))
	 (let1 sock (make-client-socket 'inet
					(~ mongo 'host)
					(~ mongo 'port))
	   (set! (~ mongo 'sock) sock)
	   (set! (~ mongo 'in) (socket-input-port sock))
	   (set! (~ mongo 'out) (socket-output-port sock)))))

(define-class <mongo-reply> ()
  ((requestID :allocation :instance :init-keyword :requestID)
   (responseTo :allocation :instance :init-keyword :responseTo)
   (CursorNotFound :allocation :instance :init-keyword :CursorNotFound)
   (QueryFailure :allocation :instance :init-keyword :QueryFailure)
   (ShardConfigStale :allocation :instance :init-keyword :ShardConfigStale)
   (AwaitCapable :allocation :instance :init-keyword :AwaitCapable)
   (cursorID :allocation :instance :init-keyword :cursorID)
   (startingFrom :allocation :instance :init-keyword :startingFrom)
   (numberReturned :allocation :instance :init-keyword :numberReturned)
   (documents :allocation :instance :init-keyword :documents)))

(define-method object-apply ((mongo-reply <mongo-reply>))
  (~ mongo-reply 'documents))

;; Constant
(define-constant OP_REPLY 1)
(define-constant OP_MSG 1000)
(define-constant OP_UPDATE 2001)
(define-constant OP_INSERT 2002)
(define-constant OP_GET_BY_OID 2003)
(define-constant OP_QUERY 2004)
(define-constant OP_GETMORE 2005)
(define-constant OP_DELETE 2006)
(define-constant OP_KILL_CURSORS 2007)

(define-method req->resp ((mongo <mongo>) buff)
  (write-block buff (~ mongo 'out))
  (flush (~ mongo 'out))
  (let* ((len (read-s32 (~ mongo 'in) 'littie-endian))
	 (recv (make-u8vector len)))
    (u8vector-copy! recv (uvector-alias <u8vector> (s32vector len)) 0)
    (read-block! recv (~ mongo 'in) 4 -1)
    recv))

(define-method req-only ((mongo <mongo>) buff)
  (write-block buff (~ mongo 'out))
  (flush (~ mongo 'out))
  (undefined))

(define (with-connection mongo proc)
  (let1 sock (~ mongo 'sock)
  (unwind-protect
   (proc (socket-input-port sock) (socket-output-port sock))
   (socket-close sock))))

(define (msg-header req-id resp-to op)
  (s32vector 0 req-id resp-to op))

(define (get-responseTo recv)
  (get-s32
   (uvector-alias <s32vector> (u8vector-copy recv 8 12))
   0 'littie-endian))

(define (make-requestID)
  (- (random-integer (+ 2147483647 2147483648)) 2147483647))

(define (check-bit num order)
  (if (equal? 
       (~ (reverse (string->list 
		    (format #f "~4,,,'0@a" 
			    (number->string num 2)))) order)
       #\1) #t
       #f))

(define (check-element-name name)
  (not (or
	(regmatch? (#/^\$.*/ name))
	(number? (string-scan name "."))
	(equal? name "_id"))))

(define (check-element-names ls)
  (unless (null? ls)
    (for-each (^a (begin 
		    (unless (check-element-name (car a))
		      (error <mongo-error> #`"element name, ',(car a)' unforbiddened."))
		    (if (list? (cdr a))
			(check-element-names (cdr a))))) ls)))

(define (write-buffer list)
  (let* ((ls (map (^a (if (u8vector? a) a (uvector-alias <u8vector> a))) list))
	 (size (fold (^ (a b) (+ (uvector-length a) b)) 0 ls))
         (buff (make-u8vector size))
         (start 0))
    (let loop ((ls ls)
               (p start))
      (if (null? ls) #f
          (begin
            (u8vector-copy! buff p (car ls))
            (loop (cdr ls) (+ p (uvector-length (car ls)))))))
    (u8vector-copy! buff (uvector-alias <u8vector> (s32vector size)) 0)
    buff))

(define (read-buffer buff)
  (let1 size (uvector-length buff)
    (let ((msg-header (s32vector->list
                       (uvector-alias <s32vector> (u8vector-copy buff 0 16))))
          (resp-flg 
	     (get-s32 (uvector-alias <s32vector> (u8vector-copy buff 16 20))
		      0 'littie-endian))
          (cursor-id (get-s64 
		      (uvector-alias <s64vector> 
				     (u8vector-copy buff 20 28)) 
		      0 'littie-endian))
          (starting-from 
	   (get-s32 (uvector-alias <s32vector> (u8vector-copy buff 28 32))
		    0 'littie-endian))
          (number-returned 
	   (get-s32 (uvector-alias <s32vector> (u8vector-copy buff 32 36))
		    0 'littie-endian))
          (docs (u8vector-copy buff 36)))
      (if (and (= (~ msg-header 3) OP_REPLY)
               (= (car msg-header) size))
	  (let1 documents 
	       (let loop ((idx 0)
			  (ls '()))
		 (let1 len (bson-length (u8vector-copy docs idx))
		   (if (not len) ls
		       (loop (+ idx len)
			     (append ls (list (bson->list (u8vector-copy docs idx))))))))
	    (if (= number-returned (length documents))
		(make  <mongo-reply> 
		  :requestID (~ msg-header 1)
		  :responseTo (~ msg-header 2) 
		  :CursorNotFound (check-bit resp-flg 0)
		  :QueryFailure (check-bit resp-flg 1)
		  :ShardConfigStale (check-bit resp-flg 2)
		  :AwaitCapable (check-bit resp-flg 3)
		  :cursorID cursor-id 
		  :startingFrom starting-from 
		  :numberReturned number-returned 
		  :documents documents)
		(error <mongo-error> "returned document number unmatched.")))
          (error <mongo-error> "Not OP_REPLY or response length unmatched.")))))


;; Basic Ops
(define-method _update ((mongo <mongo>) colname selector update . opts)
  (let-keywords opts ((Upsert :Upsert #f)
		      (MultiUpdate :MultiUpdate #f)
		      . opt)
		#;(check-element-names update)
		(let* ((requestID (make-requestID))
		       (buff (write-buffer
			      (list
			       (msg-header requestID -1 OP_UPDATE)
			       (s32vector 0)
			       (string->bson-cstr colname)
			       (s32vector (+ (if Upsert 2 0)
					     (if MultiUpdate 4 0)))
			       (list->bson selector)
			       (list->bson update)))))
		  (req-only mongo buff))))

(define-method _query ((mongo <mongo>) colname num-to-skip num-to-return query return-field-selector . opts)
  (let-keywords opts ((TailableCursor :TailableCursor #f)
		      (SlaveOk :SlaveOk #f)
		      (NoCursorTimeout :NoCursorTimeout #f)
		      (AwaitData :AwaitData #f)
		      (Exhaust :Exhaust #f)
		      (Partial :Partial #f)
                      . opt)
		(let* ((requestID (make-requestID))
		       (buff (write-buffer
			      (append
			       (list
				(msg-header requestID -1 OP_QUERY)
				(s32vector (+ (if TailableCursor 2 0)
					      (if SlaveOk 4 0)
					      (if NoCursorTimeout 16 0)
					      (if AwaitData 32 0)
					      (if Exhaust 64 0)
					      (if Partial 128 0)))
				(string->bson-cstr colname)
				(s32vector num-to-skip)
				(s32vector num-to-return)
				(list->bson query))
			       (if (not (null? return-field-selector))
				   (list (list->bson return-field-selector))
				   '()))))
		       (recv (req->resp mongo buff)))
		  (if (= requestID (get-responseTo recv))
		      (read-buffer recv)
		      (error <mongo-error> "requestID and responseTo unmatched.")))))

(define-method _insert ((mongo <mongo>) colname docs . opts)
  (let-keywords opts ((ContinueOnError :ContinueOnError #f)
		      . opt)
		#;(for-each (^a (check-element-names a)) docs)
		(let* ((requestID (make-requestID))
		       (buff (write-buffer
			      (append
			       (list
				(msg-header requestID -1 OP_INSERT)
				(s32vector (if ContinueOnError 1 0))
				(string->bson-cstr colname))
			       (map (^a (list->bson a)) docs)))))
		  (req-only mongo buff))))

(define-method _getmore ((mongo <mongo>) colname num-to-return cursor-id)
  (let* ((requestID (make-requestID))
	 (buff (write-buffer
		(list
		 (msg-header requestID -1 OP_GETMORE)
		 (s32vector 0)
		 (string->bson-cstr colname)
		 (s32vector num-to-return)
		 (s64vector cursor-id))))
	 (recv (req->resp mongo buff)))
    (if (= requestID (get-responseTo recv))
	(read-buffer recv)
	(error <mongo-error> "requestID and responseTo unmatched."))))

(define-method _delete ((mongo <mongo>) colname doc . opts)
  (let-keywords opts ((SingleRemove :SingleRemove #f)
		      . opt)
		(let* ((requestID (make-requestID))
		       (buff (write-buffer
			      (list
			       (msg-header requestID -1 OP_DELETE)
			       (s32vector 0)
			       (string->bson-cstr colname)
			       (s32vector (if SingleRemove 1 0))
			       (list->bson doc)))))
		  (req-only mongo buff))))

(define-method _kill-cusors ((mongo <mongo>) cursorIDs)
  (let* ((requestID (make-requestID))
	 (buff (write-buffer
		(append
		 (list
		  (msg-header requestID -1 OP_KILL_CURSORS)
		  (s32vector 0)
		  (s32vector (length cursorIDs)))
		 (map (^a (s64vector a)) cursorIDs)))))
    (req-only mongo buff)))

;; GridFS
(define (file->u8vector-vector filename size)
  (let1 fsize (sys-stat->size (sys-stat filename))
    (call-with-input-file filename
      (^i (let loop ((ls '()))
	    (let* ((cur (port-tell i))
		   (buff (make-u8vector 
			  (if (>= (- fsize cur) size) size 
			      (- fsize cur)))))
	      (if (= fsize cur) (list->vector ls)
		  (begin
		    (read-block! buff i)
		    (loop (append ls (list buff)))))))))))

(define (u8vector-vector->file filename vector)
  (call-with-output-file filename
    (^o (for-each (^a (write-block a o)) (vector->list vector))
	(flush o))))


;; Collection Basic Ops
(define-method find ((mongo <mongo>))
  (undefined))

(define-method insert ((mongo <mongo>))
  (undefined))

(define-method remove ((mongo <mongo>))
  (undefined))

(define-method update ((mongo <mongo>))
  (undefined))

(define-method update-1st ((mongo <mongo>))
  (undefined))

(define-method upsert ((mongo <mongo>))
  (undefined))

(define-method get-count ((mongo <mongo>))
  (undefined))

;; Collection Index Ops
(define-method create-index ((mongo <mongo>))
  (undefined))

(define-method drop-index ((mongo <mongo>))
  (undefined))

(define-method drop-indexes ((mongo <mongo>))
  (undefined))

(define-method get-index-information ((mongo <mongo>))
  (undefined))

;; Collection Misc Ops
(define-method explain ((mongo <mongo>))
  (undefined))

(define-method get-options ((mongo <mongo>))
  (undefined))

(define-method get-name ((mongo <mongo>))
  (undefined))

(define-method close ((mongo <mongo>))
  (undefined))

;; Collection Cursor Ops
(define-method get-next-document ((mongo <mongo>))
  (undefined))

(define-method get-iterator ((mongo <mongo>))
  (undefined))

(define-method has-more ((mongo <mongo>))
  (undefined))

(define-method close ((mongo <mongo>))
  (undefined))

;; Epilogue
(provide "mongo")
