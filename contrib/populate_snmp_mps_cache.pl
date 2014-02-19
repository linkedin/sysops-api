#!/usr/bin/perl
use strict;
use warnings;
use Compress::Raw::Bzip2;
use Crypt::CBC;
use Digest::MD5 qw( md5_hex );
use File::Find;
use Getopt::Long;
use JSON::XS;
use Pod::Usage;
use RedisDB;
use Sys::Hostname;

=head1 NAME

populate_snmp_mps_cache - read data from snmp device and store it in RedisDB

=head1 DESCRIPTION

read data from snmp device via snmpwalk and store data in RedisDB for sysops-api

=head1 SYNOPSIS

 populate_snmp_mps_cache

 Options:

  --verbose Enable verbose execution.

  --hostname to contact for snmp request.

  --community to use. if ommited used default from config file.

  --server server with redisdb. defaults to localhost

  --help display help.

  --man display manpage.

  --oids object identifers seperated by , eg system,ifTable,ifPhys

=head1 EXAMPLES

You might want to run this script via cron in a script like this:

 for host in `host -l bundesbrandschatzamt.de | grep network.bundesbrandschatzamt.de | awk -F\  '{print $1}'`; do
  /var/cfengine/bin/populate_snmp_mps_cache --hostname ${host} --oids ifTable,system,ifPhys
 done

Consider using /root/.snmp/snmp.conf to hide your community credentials.

=head1 SUBS

=head2 main

=cut

sub main {
    my %params;

    my $snmpwalk;

    my $cipher = Crypt::CBC->new( -key => '${shared_global_environment.sysopskey}',
				  -cypher => 'DES_EDE3'
				);

    $params{server} = "localhost";

    my $result = GetOptions (
			     "help|h"		=> \$params{help},
			     "verbose|v"	=> \$params{verbose},
			     "man|m"		=> \$params{man},
			     "server|s=s"	=> \$params{server},
			     "hostname=s"	=> \$params{hostname},
			     "oids=s"		=> \$params{oids},
			     "community|c=s"	=> \$params{community}
	);

    if ( $params{man} ) { pod2usage( -verbose => 2 ); }

    if ( $params{help} || ! defined $params{hostname} )
    {
	pod2usage(1);
    }


    if ( $params{community} )
      {
	$snmpwalk = "/usr/bin/snmpwalk -t 1 -r 5 -c $params{community}";
      } else
	{
	  $snmpwalk = "/usr/bin/snmpwalk -t 1 -r 5";
	}

    my $redis = RedisDB->new( host => "$params{server}",
			      port => 6379,
			      database => 1
			    );

    foreach my $oid ( split(/,/, $params{oids}) )
      {
	my ( $filecontent, $filecontentcompressed, @wc_words );

	open SNMP, "$snmpwalk $params{hostname} $oid |" or die "couldn't run snmpwalk";

	while ( my $line = <SNMP> )
	  {
	    $filecontent = $filecontent . $line;
	  }

	close SNMP;

	if ( length( $filecontent ) )
	  {
	    my ($bz, $status) = new Compress::Raw::Bzip2 1, 9, 30
	      or die "Cannot create bzip2 object\n";

	    $status = $bz->bzdeflate( $cipher->encrypt( $filecontent ), $filecontentcompressed );
	    $status = $bz->bzflush( $filecontentcompressed );
	    $status = $bz->bzclose( $filecontentcompressed );

	    @wc_words = split(/[ \t\n]+/, $filecontent);

	    my $json = { content => $filecontentcompressed,
			 md5sum => md5_hex( $filecontent ),
			 wc => "Number of characters: " .  (length( $filecontent) ) . " Number of lines: " . ( $filecontent =~ tr/\n// ) . " Number of words: " . @wc_words,
		       };

	    $redis->set( $params{hostname} . '#' . $oid, encode_json $json);
	    $redis->expire( $params{hostname} . '#' . $oid, 2592000 ); # 2592000 = 30 days
	  }
      }

}

main();

=head1 AUTHOR


Andreas Gerler <baron@bundesbrandschatzamt.de>

=head1 LICENSE

This program is free software, you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
