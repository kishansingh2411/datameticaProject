#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: shopping_cart_util.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute the java program written to fetch the shopping cart information from AMSS
#   Author(s)   : DataMetica Team
#   Usage       : sh shopping_cart_util.sh <shopping_cart_util_jar_file_path> <config_properties_file_path> <log_dir_path> 
#   Date        : 03/13/2016
#   Log File    : <path_to_log_dir>/shopping_cart_util.log
#   SQL File    : 
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          03/13/2016       Initial version
#
#
#####################################################################################################################

no_of_arguments=$#
echo "Arguments passed to the script $*"
echo "Java home is set to $INFA_JDK_HOME"

if [ $no_of_arguments -eq 3 ]
then
    $INFA_JDK_HOME/bin/java -Xms1024m -Xmx2500m -jar $1 $2 $3
else
    $INFA_JDK_HOME/bin/java -Xms1024m -Xmx2500m -jar $1 $2
fi



##############################################################################
#                                    End                                     #
##############################################################################
