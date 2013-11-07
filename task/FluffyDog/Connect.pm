package FluffyDog::Connect;
use strict;
use warnings;

use DBI;

# Magic numbers.
use constant ORACLE =>  1;
use constant MYSQL  =>  2;
use constant SYBASE =>  3;              # aka SQL Server

use constant ENVIRO =>  50;
use constant DIRECT =>  51;

our %databases = (
    'BACKEND'           =>  [ ORACLE, ENVIRO, 'FLUFFYDOG_DB',  'FLUFFYDOG_USER',            'FLUFFYDOG_PASSWORD'           ],
    'FLUFFYEND'           =>  [ MYSQL, ENVIRO, 'FLUFFYDOG_HOST', 'FLUFFYDOG_USER',            'FLUFFYDOG_PASSWORD'           ],
    'CTX_CVARCH'        =>  [ ORACLE, ENVIRO, 'DB_CV_ARCHIVE', 'FLUFFYDOG_CTX_SEARCH_USER', 'FLUFFYDOG_CTX_SEARCH_PASSWORD'],
);

sub connect_live                {       return  connect_database('BACKEND')    }
sub connect_local               {       return  connect_database('FLUFFYEND')  }
sub connect_ctx_cvarch          {       return  connect_database('CTX_CVARCH') }


# connect_database( Code );
# Handler to connect to any database that we know about.
sub connect_database
{
    my $database_key = shift || '';
    my $special_params = shift || {};

    if (!$database_key)
    {
        die("connect_database(): No database name specified.");
    }

    # Extract connection details from %database_details
    my @database_details = @{$databases{$database_key}};
    my ($database_type, $database_mode, $database_host, $database_user, $database_password, $database_name) = @database_details;

    $database_name = "" unless $database_name;

    my $param_database_host = "";
    my $param_database_user = "";
    my $param_database_password = "";
    my $param_database_name = "";

    # If this is an ENV mode connection, then grab the details from system ENV variables.
    if ($database_mode eq ENVIRO)
    {
        $param_database_host = $ENV{$database_host};
        $param_database_user = $ENV{$database_user};
        $param_database_password = $ENV{$database_password};
        $param_database_name = $ENV{$database_name} || '';
    }
    # .. otherwise we treat them 'raw'...
    else
    {
        $param_database_host = $database_host;
        $param_database_user = $database_user;
        $param_database_password = $database_password;
        $param_database_name = $database_name;
    }

    if ($database_type eq ORACLE)
    {
        return connect_database_oracle($param_database_host, $param_database_user, $param_database_password);
    }
    elsif ($database_type eq MYSQL)
    {
        return connect_database_mysql($param_database_host, $param_database_user, $param_database_password, $param_database_name, $special_params);
    }
}

sub connect_database_oracle
{
    my ($database_host, $database_user, $database_password) = @_;

#print STDERR "host: $database_host\n";
#print STDERR "user: $database_user\n";
#print STDERR "passwd: $database_password\n";

    my $connection = DBI->connect("dbi:Oracle:".$database_host, $database_user, $database_password,{ora_envhp => 0, AutoCommit => 0});

    unless($connection)
    {
        print STDERR "NLS_LANG = [", $ENV{'NLS_LANG'}, "]\n";
        print STDERR "ORACLE_HOME = [", $ENV{'ORACLE_HOME'}, "]\n";

        my $fn = $ENV{ORACLE_HOME}. "/network/admin/tnsnames.ora";
        open FH, $fn or die "cannot open $fn";
        local $\=undef;
        local $/=undef;
        my $a = <FH>;
        close FH;
    } 
    else
    {
        # Match Session Settings To Oracle NLS_LANG Environment Variable
        my ($nls_language,$nls_territory)= $ENV{'NLS_LANG'} =~/(.+)_(.+)\./; 
            $nls_language = uc($nls_language); 
            $nls_territory = uc($nls_territory);

        $connection->do(qq{alter session set nls_language = '$nls_language'}); 
        $connection->do(qq{alter session set nls_territory = '$nls_territory'}); 

        $connection->{LongReadLen} = 10000000;
        $connection->{LongTruncOk} = 1;
        $connection->{RaiseError} = 1;      # error->STDERR, then die...
        $connection->{PrintError} = 0;      # error already outputted by RaiseError
    }

    return $connection;
}

# connect_database_mysql (Host, User, Password, Database name)
# Standard MySQL database connection handler.  Invoked by connect_database()
sub connect_database_mysql
{
    my ($database_host, $database_user, $database_password, $database_name, $special_params) = @_;

#print STDERR "host: $database_host\n";
#print STDERR "user: $database_user\n";
#print STDERR "passwd: $database_password\n";
#print STDERR "name: $database_name\n";

    my $connection = DBI->connect("dbi:mysql:" . $database_name . ":" . $database_host, $database_user, $database_password, {AutoCommit => 0});

    if ($connection)
    {
        if (exists $special_params->{'return_y'} && $special_params->{'return_y'} eq 'Y')
        {
            return ($connection,'Y');
        }
        else
        {
            return $connection;
        }
    }
    else
    {
        return undef;
    }
}
1;
