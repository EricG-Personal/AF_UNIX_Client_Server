

#include <launch.h>
#include <libkern/OSAtomic.h>
#include <vproc.h>
#include "shared.h"
#include <asl.h>

static bool g_is_managed = false;
static bool g_accepting_requests = true;
static dispatch_source_t g_timer_source = NULL;

aslclient	gASL		= NULL;
aslmsg		gASLMessage = NULL;


/* OSAtomic*() requires signed quantities. */
static int32_t g_transaction_count = 0;

int server_check_in(void)
{
	int sockfd = -1;
	
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server_check_in" );
				
	/* If we're running under a production scenario, then we check in with
	 * launchd to get our socket file descriptors.
	 */
	launch_data_t req = launch_data_new_string(LAUNCH_KEY_CHECKIN);
	assert(req != NULL);
	
	launch_data_t resp = launch_msg(req);
	assert(resp != NULL);
	assert(launch_data_get_type(resp) == LAUNCH_DATA_DICTIONARY);

	launch_data_t sockets = launch_data_dict_lookup(resp, LAUNCH_JOBKEY_SOCKETS);
	assert(sockets != NULL);
	assert(launch_data_get_type(sockets) == LAUNCH_DATA_DICTIONARY);
	
	launch_data_t sarr = launch_data_dict_lookup(sockets, "net.ericgorr.testserver.sock");
	assert(sarr != NULL);
	assert(launch_data_get_type(sarr) == LAUNCH_DATA_ARRAY);
	
	size_t count = launch_data_array_get_count( sarr );
			
	launch_data_t socketID = launch_data_array_get_index( sarr, 0 );
	
	sockfd = launch_data_get_fd(socketID);

	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "sockfd: %d", sockfd );

	return sockfd;
}

vproc_transaction_t server_open_transaction(void)
{
	/* Atomically increment our count of outstanding requests. Even though
	 * this happens serially, remember that requests themselves are handled
	 * concurrently on GCD's default priority queue. So when the requests are
	 * closed out, it can happen asynchronously with respect to this section
	 * Thus, any manipulation of the transaction counter needs to be guarded.
	 */
	if (  OSAtomicIncrement32(&g_transaction_count ) - 1 == 0 ) 
	{
		dispatch_source_set_timer(g_timer_source, DISPATCH_TIME_FOREVER, 0llu, 0llu);
	}
	
	/* Open a new transaction. This tells Instant Off that we are "dirty" and
	 * should not be sent SIGKILL if the time comes to shut the system down.
	 * Instead, we will be sent SIGTERM.
	 */
	
	return vproc_transaction_begin(NULL);
}

void server_close_transaction(vproc_transaction_t vt)
{
	if ( OSAtomicDecrement32(&g_transaction_count ) == 0 ) 
	{
		dispatch_time_t t0 = dispatch_time( DISPATCH_TIME_NOW, 20llu * NSEC_PER_SEC );
		
		dispatch_source_set_timer( g_timer_source, t0, 0llu, 0llu );
	}
	
	vproc_transaction_end( NULL, vt );
}

void server_send_reply(int fd, dispatch_queue_t q, CFDataRef data, vproc_transaction_t vt)
{
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server_send_reply" );

	size_t			total	= sizeof( struct ss_msg_s ) + CFDataGetLength( data );	
	unsigned char*	buff	= (unsigned char *)malloc(total);
	
	assert(buff != NULL);

	struct ss_msg_s *msg = (struct ss_msg_s *)buff;
	
	msg->_len = OSSwapHostToLittleInt32(total - sizeof(struct ss_msg_s));

	/* Coming up with a more efficient implementation is left as an exercise to
	 * the reader.
	 */
	(void)memcpy( msg->_bytes, CFDataGetBytePtr( data ), CFDataGetLength( data ) );
	
	dispatch_source_t s = dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE, fd, 0, q);
	assert(s != NULL);
	
	__block unsigned char*	track_buff	= buff;
	__block size_t			track_sz	= total;
	
	dispatch_source_set_event_handler(s, ^(void) 
	{
		ssize_t nbytes = write(fd, track_buff, track_sz);
		if (nbytes != -1) 
		{
			track_buff += nbytes;
			track_sz -= nbytes;
			
			asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_WRITE - writing bytes" );
			
			if ( track_sz == 0 ) 
			{
				asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_WRITE - all bytes written" );

				dispatch_source_cancel( s );
			}
		}
	});

	dispatch_source_set_cancel_handler(s, ^(void) 
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_WRITE - canceled" );
		
		dispatch_release(s);
	});
	
	dispatch_resume(s);
}

Boolean CFStringGetCString (
							CFStringRef theString,
							char *buffer,
							CFIndex bufferSize,
							CFStringEncoding encoding
							);


void LogCFStringRef( char* theMessage, CFStringRef theString )
{
	char theCString[1024];
	
	if ( CFStringGetCString( theString, theCString, 1024, kCFStringEncodingUTF8 ) )
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "%s: %s", theMessage, theCString );
	}
}

void server_handle_request(int fd, const void *buff, size_t total, vproc_transaction_t vt)
{
    CFDataRef data = CFDataCreateWithBytesNoCopy( NULL, buff, total, kCFAllocatorNull );
	assert(data != NULL);
	
    CFStringRef receivedMessage = CFStringCreateFromExternalRepresentation( NULL, data, kCFStringEncodingUTF8 );
	assert( receivedMessage != NULL );

	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server_handle_request" );
	LogCFStringRef( "IT WAS", receivedMessage );
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "*********************" );
		
	CFStringRef theReply	= CFStringCreateWithFormat( NULL, NULL, CFSTR( "Reply To: %@" ), receivedMessage );
    CFDataRef	replyData	= CFStringCreateExternalRepresentation( NULL, theReply, kCFStringEncodingUTF8, 0 );
	assert( replyData != NULL );

	LogCFStringRef( "sending reply", theReply );

    CFRelease( data );
    CFRelease( theReply );
	
    server_send_reply( fd, dispatch_get_current_queue(), replyData, vt );

	/* ss_send_reply() copies the data from replyData out, so we can safely
	 * release it here. But remember, that's an inefficient design.
	 */
	CFRelease( replyData );
}



bool server_read( int fd, unsigned char *buff, size_t buff_sz, void** msgStart, size_t *total, vproc_transaction_t vt )
{
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server_read" );

	bool result = false;
	
	struct ss_msg_s *msg = (struct ss_msg_s *)buff;
	
	size_t			headerSize	= sizeof( struct ss_msg_s ); 
	unsigned char*	track_buff	= buff + *total;
	size_t			track_sz	= buff_sz - *total;
	ssize_t			nbytes		= read( fd, track_buff, track_sz );

	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "nbytes: %lu", nbytes );
	
	if ( nbytes == 0 )
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "all bytes read" );
		
		return true;
	}
	else if ( nbytes == -1 )
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server_read: error on read!" );
		
		return true;
	}
	else
	{
		*total += nbytes;

		/* We do this swap on every read(2), which is wasteful. But there is a
		 * way to avoid doing this every time and not introduce an extra
		 * parameter. See if you can find it.
		 */
		if ( *total >= headerSize ) 
		{
			msg->_len = OSSwapLittleToHostInt32( msg->_len );
			
			while ( *total >= headerSize && *total - headerSize >= msg->_len )
			{
				asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "Have Message" );
								
				server_handle_request( fd, msg->_bytes, msg->_len, vt );
	
				*total		= *total - ( headerSize + msg->_len );
				*msgStart	= buff + headerSize +  msg->_len;
				msg			= *msgStart;
			}			
		}
	}

	return result;
}

void server_accept(int fd, dispatch_queue_t q)
{
	/* This variable needs to be mutable in the block. Setting __block will
	 * ensure that, when dispatch_source_set_event_handler(3) copies it to
	 * the heap, this variable will be copied to the heap as well, so it'll
	 * be safely mutable in the block.
	 */
	__block size_t total = 0;

	vproc_transaction_t vt = server_open_transaction();
	
	/* For large allocations like this, the VM system will lazily create
	 * the pages, so we won't get the full 10 MB (or anywhere near it) upfront.
	 * A smarter implementation would read the intended mess_age size upfront
	 * into a fixed-size buffer and then allocate the needed space right there.
	 * But if our requests are almost always going to be this small, then we
	 * avoid a potential second trap into the kernel to do the second read(2).
	 * Also, we avoid a second copy-out of the data read.
	 */
	size_t	buff_sz		= 10 * 1024 * 1024;
	void*	buff		= malloc( buff_sz );
	void*	msgStart	= buff;
	
	assert(buff != NULL);

	dispatch_source_t s = dispatch_source_create( DISPATCH_SOURCE_TYPE_READ, fd, 0, q );
	assert(s != NULL);

	dispatch_source_set_event_handler(s, ^(void) 
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ B" );
		
		/* You may be asking yourself, "Doesn't the fact that we're on a concurrent
		 * queue mean that multiple event handler blocks could be running
		 * simultaneously for the same source?" The answer is no. Parallelism for
		 * the global concurrent queues is at the source level, not the event
		 * handler level. So for each source, exactly one invocation of the event
		 * handler can be inflight. When scheduling on a concurrent queue, it
		 * means that that handler may be running concurrently with other sources'
		 * event handlers, but not its own.
		 */
		if ( server_read( fd, buff, buff_sz, &msgStart, &total, vt ) ) 
		{
			asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ B - server_read success" );
			
			/* After handling the request (which, in this case, means that we've
			 * scheduled a source to deliver the reply), we no longer need this
			 * source. So we cancel it.
			 */
			dispatch_source_cancel(s);
		}
	});

	dispatch_source_set_cancel_handler(s, ^(void) 
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ B - canceled" );
	
		dispatch_release( s );
		free( buff );
		close( fd );
		
		server_close_transaction( vt );
	});
	
	dispatch_resume(s);
}



int main( int argc, const char *argv[] )
{
	gASL		= asl_open( "SSD", "SSD Facility", ASL_OPT_STDERR ) ;
	gASLMessage = asl_new( ASL_TYPE_MSG );
	
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "Server Starting" );
	
	/* An argv[1] of "launchd" indicates that were were launched by launchd.
	 * Note that we ONLY do this check for debugging purposes. There should be no
	 * production scenario where this daemon is not being managed by launchd.
	 */
	if (argc > 1 && strcmp(argv[1], "launchd") == 0) 
	{
		g_is_managed = true;
	} else 
	{
		/* When running under a debugging environment, log mess_ages to stderr. */
		(void)openlog("wwdc-debug", LOG_PERROR, 0);
	}
	
	/* This daemon handles events serially on the main queue. The events that
	 * are synchronized on the main queue are:
	 * • New connections
	 * • The idle-exit timer
	 * • The SIGTERM handler
	 *
	 * Note that actually handling requests is done concurrently.
	 */
	dispatch_queue_t mq = dispatch_get_main_queue();
	
	g_timer_source = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, mq);
	
	assert(g_timer_source != NULL);
	
	/* When the idle-exit timer fires, we just call exit(2) with status 0. */
	dispatch_set_context(g_timer_source, NULL);
	dispatch_source_set_event_handler_f(g_timer_source, (void (*)(void *))exit);
	/* We start off with our timer armed. This is for the simple reason that,
	 * upon kicking off the GCD state engine, the first thing we'll get to is
	 * a connection on our socket which will disarm the timer. Remember, handling
	 * new connections and the firing of the idle-exit timer are synchronized.
	 */
	dispatch_time_t t0 = dispatch_time(DISPATCH_TIME_NOW, 20llu * NSEC_PER_SEC);
	dispatch_source_set_timer(g_timer_source, t0, 0llu, 0llu);
	dispatch_resume(g_timer_source);
	
	/* We must ignore the default action for SIGTERM so that GCD can safely receive it
	 * and distribute it across all interested parties in the address space.
	 */
	(void)signal(SIGTERM, SIG_IGN);
	
	/* For Instant Off, we handle SIGTERM. Since SIGTERM is Instant Off's way of
	 * saying "Wind down your existing requests, and don't accept any new ones",
	 * we set a global saying to not accept new requests. This source fires
	 * synchronously with respect to the source which monitors for new connections
	 * on our socket, so things will be neatly synchronized. So unless_ it takes
	 * us 20 seconds between now and when we call dispatch_main(3), we'll be okay.
	 */
	dispatch_source_t sts = dispatch_source_create(DISPATCH_SOURCE_TYPE_SIGNAL, SIGTERM, 0, mq);
	assert(sts != NULL);
	
	dispatch_source_set_event_handler(sts, ^(void) 
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_SIGNAL" );
		
		/* If we get SIGTERM, that means that the system is on its way out, so
		 * we need to close out our existing requests and stop accepting new
		 * ones. At this point, we know that we have at least one outstanding
		 * request. Had we been clean, we would've received SIGKILL and just
		 * exited.
		 *
		 * Note that by adopting Instant Off, you are opting into a contract
		 * where you assert that launchd is the only entity which can
		 * legitimately send you SIGTERM.
		 */
		g_accepting_requests = false;
	});
	
	dispatch_resume(sts);
	
	/* Now that we've set all that up, get our socket. */
	int fd = server_check_in();

	/* This is REQUIRED for GCD. To understand why, consider the following scenario:
	 * 0. GCD monitors the descriptor for bytes to read.
	 * 1. Bytes appear, so GCD fires off every source interested in whether there
	 *    are bytes on that socket to read.
	 * 2. 1 of N sources fires and consumes all the outstanding bytes on the
	 *    socket by calling read(2).
	 * 3. The other N - 1 sources fire and each attempt a read(2). Since all the
	 *    data has been drained, each of those read(2) calls will block.
	 *
	 * This is highly undesirable. It is important to remember that parking a
	 * queue in an unbounded blocking call will prevent any other source that
	 * fires on that queue from doing so. So whenever poss_ible, we must avoid
	 * unbounded blocking in event handlers.
	 */
	(void)fcntl(fd, F_SETFL, O_NONBLOCK);
		
	/* DISPATCH_SOURCE_TYPE_READ, in this context, means that the source will
	 * fire whenever there is a connection on the socket waiting to be accept(2)ed.
	 * I know what you're thinking after reading the above comment. "Doesn't this
	 * mean that it's safe for this socket to be blocking? The source won't fire
	 * until there is a connection to be accept(2)ed, right?"
	 *
	 * This is true, but it is important to remember that the client on the other
	 * end can cancel its attempt to connect. If the source fires after this has
	 * happened, accept(2) will block. So it is still important to set O_NONBLOCK
	 * on the socket.
	 */
	dispatch_source_t as = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ, fd, 0, mq);
	assert(as != NULL);

	dispatch_source_set_event_handler(as, ^(void) 
	{
		struct sockaddr saddr;
		socklen_t		slen	= 0;

		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ A" );

		int afd = accept( fd, (struct sockaddr *)&saddr, &slen );
		
		if ( afd != -1 ) 
		{
			asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ A - accepted" );

			/* Again, make sure the new connection's descriptor is non-blocking. */
			(void)fcntl( fd, F_SETFL, O_NONBLOCK );
			
			/* Check to make sure that we're still accepting new requests. */
			if (g_accepting_requests) 
			{
				/* We're going to handle all requests concurrently. This daemon uses an HTTP-style
				 * model, where each request comes through its own connection. Making a more
				 * efficient implementation is an exercise left to the reader.
				 */
				server_accept( afd, dispatch_get_global_queue( DISPATCH_QUEUE_PRIORITY_DEFAULT, 0 ) );
			} 
			else 
			{
				/* We're no longer accepting requests. */
				(void)close(afd);
			}
		}
	});

	/* GCD requires that any source dealing with a file descriptor have a
	 * cancellation handler. Because GCD needs to keep the file descriptor
	 * around to monitor it, the cancellation handler is the client's signal
	 * that GCD is done with the file descriptor, and thus the client is safe
	 * to close it out. Remember, file descriptors aren't ref counted.
	 */
	dispatch_source_set_cancel_handler(as, ^(void) 
	{
		asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "DISPATCH_SOURCE_TYPE_READ A - canceled" );
		
		dispatch_release( as );
		(void)close( fd );
	});
	
	dispatch_resume( as );
	
	asl_log( gASL, gASLMessage, ASL_LEVEL_INFO, "server - dispatch_main" );
	
	dispatch_main();
	
	exit( EXIT_FAILURE );
}
