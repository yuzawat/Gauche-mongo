;;;
;;; mongo
;;;

(define-module mongo
  (use gauche.uvector)
  (use rfc.json)
  (use gauche.net)
  (use binary.io)
  (use srfi-1)
  (use srfi-27)
  (use mongo.bson)
  (export
   <mongo-connection>
   <mongo-error>
   req->resp
   make-requestID
   get-responseTo
   write-buffer
   read-buffer
   query
   update
   insert
   remove
   update-first
   upsert
   get-count))

(select-module mongo)

(define-condition-type <mongo-error> <error>
  mongo-error?
  (reason mongo-error-reason))

(define-class <mongo-connection> ()
  ((host :allocation :instance
         :init-keyword :host
         :init-value "localhost")
   (port :allocation :instance
         :init-keyword :port
         :init-value 27017)
   (in :allocation :instance)
   (out :allocation :instance)
   (sock :allocation :instance)))

(define-method initialize ((mongo <mongo-connection>) initargs)
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
(define-constant Upsert 0)
(define-constant MultiUpdate 1)
(define-constant ContinueOnError 0)
(define-constant TailableCursor 1)
(define-constant SlaveOk 2)
(define-constant OplogReplay 3)
(define-constant NoCursorTimeout 4)
(define-constant AwaitData 5)
(define-constant Exhaust 6)
(define-constant Partial 7)
(define-constant SingleRemove 1)
(define-constant CursorNotFound 0)
(define-constant QueryFailure 1)
(define-constant ShardConfigStale 2)
(define-constant AwaitCapable 3)

(define-method req->resp ((mongo <mongo-connection>) buff)
  (write-block buff (~ mongo 'out))
  (flush (~ mongo 'out))
  (let* ((len (read-s32 (~ mongo 'in) 'littie-endian))
	 (recv (make-u8vector len)))
    (u8vector-copy! recv (uvector-alias <u8vector> (s32vector len)) 0)
    (read-block! recv (~ mongo 'in) 4 -1)
    recv))

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
    #;(if (<= size 48)
        (error <mongo-error> "OP_REPLY Too Short"))
    (let ((msg-header (s32vector->list
                       (uvector-alias <s32vector> (u8vector-copy buff 0 16))))
          (resp-flg 
	   (string->list
	    (number->string
	     (get-s32 (uvector-alias <s32vector> (u8vector-copy buff 16 20))
		      0 'littie-endian) 2)))
          (cursor-id (uvector-alias <s64vector> (u8vector-copy buff 20 28)))
          (starting-from (uvector-alias <s32vector> (u8vector-copy buff 28 32)))
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
		(list msg-header resp-flg cursor-id
		      starting-from number-returned documents)
		(error <mongo-error> "returned document number unmatched.")))
          (error <mongo-error> "Not OP_REPLY or response length unmatched.")))))

(define-method query ((mongo <mongo-connection>) colname num-to-skip num-to-return query . opts)
  (let-keywords opts ((TailableCursor :TailableCursor #f)
		      (SlaveOk :SlaveOk #f)
		      (NoCursorTimeout :NoCursorTimeout #f)
		      (AwaitData :AwaitData #f)
		      (Exhaust :Exhaust #f)
		      (Partial :Partial #f)
                      . opt)
		(let* ((requestID (make-requestID))
		       (buff (write-buffer
			      (list
			       (msg-header requestID 2 OP_QUERY)
			       (s32vector (+ (if TailableCursor 2 0)
					     (if SlaveOk 4 0)
					     (if NoCursorTimeout 16 0)
					     (if AwaitData 32 0)
					     (if Exhaust 64 0)
					     (if Partial 128 0)))
			       (string->bson-cstr colname)
			       (s32vector num-to-skip)
			       (s32vector num-to-return)
			       (list->bson #?=query))))
		       (recv (req->resp mongo buff)))
		  (if (= requestID (get-responseTo recv))
		      (read-buffer recv)
		      (error <mongo-error> "requestID and responseTo unmatched.")))))

(define-method insert ((mongo <mongo-connection>) colname docs)
  (let* ((requestID (make-requestID))
	 (buff (write-buffer
		(append
		 (list
		  (msg-header requestID 2 OP_INSERT)
		  (s32vector 0)
		  (string->bson-cstr colname))
		 (map (^a (list->bson a)) docs))))
	 (recv (req->resp mongo buff)))
    (if (= requestID (get-responseTo recv))
	(read-buffer recv)
	(error <mongo-error> "requestID and responseTo unmatched."))))

;; Epilogue
(provide "mongo")
