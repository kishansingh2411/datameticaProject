#!/bin/bash

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: module.sh
#   Program type: Unix KBash Shell script
#   Purpose:    : Module script to implement logic in pig script for validation 
#                 
#   Author(s)   : DataMetica Team
#   Usage       : " 
#				: 
#
#   Date        : 09/12/2016
#   Log File    : 
#   SQL File    : 
#   Error File  : 
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          09/12/2016       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Logic to be implemented to remove Carriage         #
###############################################################################

sed '
:join
/\\$/{N
s/\\\n//
b join
}'
