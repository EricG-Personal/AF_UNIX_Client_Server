#include <mach-o/dyld.h>
#include <spawn.h>
#include <sys/un.h>
#include <vproc.h>
#include "shared.h"
#include "common.h"



int client_connect( void )
{
	int socketFD = socket( AF_UNIX, SOCK_STREAM, 0 );
	assert( socketFD != -1 );
		
	struct sockaddr_un addr;
	
	addr.sun_len    = sizeof( addr );
	addr.sun_family = AF_UNIX;
	
	strcpy( addr.sun_path, kServerSocketPath );
	
	int result;
	int err;
	
	result	= connect( socketFD, (struct sockaddr *)&addr, sizeof( addr ) );
	err		= MoreUNIXErrno( result );
	CFShow( CFStringCreateWithFormat( NULL, NULL, CFSTR( "client_connect: %d" ), err ) );
	
	assert( err == 0 );
	
	return socketFD;
}



bool client_write_request( int fd, CFDataRef data )
{
	size_t			space	= 10 * 1024 * 1024;
	unsigned char*	buff	= (unsigned char *)malloc(space);
	
	assert( buff != NULL );	
	assert( CFDataGetLength(data) <= space );
	
	/* We're going to use little endian as our wire encoding. This only matters
	 * for the first word of the data sent, which specifies the length. Everything
	 * that follows is not endian-sensitive.
	 *
	 * Note that OSSwapHostToLittleInt32() implicitly casts the input to uint32_t,
	 * despite the name indicating that it works with a signed 32-bit quantity.
	 */


	dispatch_queue_t dispatchQueue = dispatch_get_main_queue();
	
	struct ss_msg_s*	msg		= (struct ss_msg_s *)buff;
	
	CFIndex				length	= OSSwapHostToLittleInt32(CFDataGetLength(data));
	msg->_len = (uint32_t)length;

	(void)memcpy( msg->_bytes, CFDataGetBytePtr( data ), CFDataGetLength( data ) );

	(void)fcntl( fd, F_SETFL, O_NONBLOCK );

	dispatch_source_t writeSource = dispatch_source_create( DISPATCH_SOURCE_TYPE_WRITE, fd, 0, dispatchQueue );
	assert( writeSource != NULL );
	
	__block unsigned char*	track_buff	= buff;
	__block size_t			track_sz	= sizeof(struct ss_msg_s) + length;

	dispatch_source_set_event_handler( writeSource, ^(void) 
	{
		CFShow( CFSTR( "client_write_request: writing" ) );

		(void)fcntl( fd, F_SETFL, O_NONBLOCK );
		
		ssize_t nbytes = write( fd, track_buff, track_sz );
		
		if ( nbytes != -1 ) {
			track_buff	+= nbytes;
			track_sz	-= nbytes;
			
			if ( track_sz == 0 ) 
			{
				CFStringRef nbytesReadString = CFStringCreateWithFormat( NULL, NULL, CFSTR( "client_write_request: all bytes written: %lu" ),  sizeof(struct ss_msg_s) + length );
				CFShow(	nbytesReadString );
				CFRelease( nbytesReadString );
				
				dispatch_source_cancel( writeSource );
			}
		}
	} );


	dispatch_source_set_cancel_handler( writeSource, ^(void) 
	{
		CFShow( CFSTR( "client_write_request: DISPATCH_SOURCE_TYPE_WRITE - canceled" ) );
	   
		free( buff );
		dispatch_release( writeSource );
		CFRelease( data );
		
		(void)fcntl( fd, F_SETFL, O_NONBLOCK );
    });
	
	dispatch_resume( writeSource );
	
	return true;
}



bool client_send_request( int fd, CFStringRef message )
{
	CFDataRef data = CFStringCreateExternalRepresentation( NULL, message, kCFStringEncodingUTF8, 0 );
	assert(data != NULL);
	
	CFShow( CFSTR( "client_send_request" ) );
	CFShow( data );
	
	client_write_request( fd, data );
	
	return true;
}



void client_process_read( const void *buff, size_t total )
{
	if ( total > 0 )
	{
		CFDataRef data = CFDataCreateWithBytesNoCopy( NULL, buff, total, kCFAllocatorNull );
		assert( data != NULL );
		
		CFStringRef receivedMessage = CFStringCreateFromExternalRepresentation( NULL, data, kCFStringEncodingUTF8 );
		assert( receivedMessage != NULL );
				
		CFShow( CFSTR( "***** client_process_read: RECEIVED MESSAGE" ) );
		CFShow( receivedMessage );
		
		CFRelease( data );
		CFRelease( receivedMessage );
	}
}



bool client_read_replies( int fd )
{
	dispatch_source_t as = dispatch_source_create( DISPATCH_SOURCE_TYPE_READ, fd, 0, dispatch_get_main_queue() );
	assert(as != NULL);

	
	size_t	headerSize	= sizeof( struct ss_msg_s ); 
	size_t	buff_sz		= 10 * 1024 * 1024;
	void*	buff		= calloc( 1, buff_sz );
	
	assert( buff != NULL );

	__block size_t	total		= 0;
	__block void*	msgStart	= buff;

	dispatch_source_set_event_handler(as, ^(void) 
    {
		(void)fcntl( fd, F_SETFL, O_NONBLOCK );
		
		CFShow( CFSTR( "client_read_replies: DISPATCH_SOURCE_TYPE_READ A" ) );
		
		struct ss_msg_s *msg = (struct ss_msg_s *)msgStart;
		
		unsigned char*	track_buff	= buff + total;
		size_t			track_sz	= buff_sz - total;
		ssize_t			nbytes		= read(fd, track_buff, track_sz);
		
		printf( "Read: %lu (%lu)\n", nbytes, total );
		
		if ( nbytes == 0 )
		{
			dispatch_source_cancel( as );
		}
		else if ( nbytes == -1 )
		{
			dispatch_source_cancel( as );
		}
		else
		{
			total += nbytes;

			if ( total >= headerSize ) 
			{
				msg->_len = OSSwapLittleToHostInt32( msg->_len );
				printf( "msg->_len: %u\n", msg->_len );
			
				while ( total >= headerSize && total - headerSize >= msg->_len )
				{
					printf( "have reply: msg->_len: %u\n", msg->_len);
					
					client_process_read( msg->_bytes, msg->_len );
					
					total		= total - ( headerSize + msg->_len );
					msgStart	= buff + headerSize +  msg->_len;
					msg			= (struct ss_msg_s *)msgStart;
				}			
			}
			
			printf( "processed read\n" );
		}
	});
	
	dispatch_source_set_cancel_handler(as, ^(void) 
    {
		CFShow( CFSTR( "client_read_replies: DISPATCH_SOURCE_TYPE_READ A - canceled" ) );
	   
		dispatch_release( as );

		free( buff );
		
		client_read_replies( fd );
   });
	
	dispatch_resume( as );
	
	return true;
}



int main(int argc, const char *argv[])
{
	CFShow( CFSTR( "Client" ) );

	CFStringRef	firstMessage	= CFStringCreateWithFormat( NULL, NULL, CFSTR( "First Message" ) );
	CFStringRef	secondMessage	= CFStringCreateWithFormat( NULL, NULL, CFSTR( "Second Message" ) );
	
	CFShow( firstMessage );
	CFShow( secondMessage );
	
	int sfd = client_connect();
	assert(sfd != -1);
	
	CFShow( CFSTR( "Client Connected" ) );
	
	(void)fcntl(sfd, F_SETFL, O_NONBLOCK);
	
	assert( client_read_replies( sfd ) );
	assert( client_send_request( sfd, firstMessage ) );
	assert( client_send_request( sfd, secondMessage ) );
	
	CFShow( CFSTR( "dispatch_main" ) );
	
	dispatch_main();
	
	fprintf(stdout, "Sweet! We didn't crash!\n");
	
	//(void)close( sfd );
	
	return 0;
}
