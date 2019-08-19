#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: executor.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Execute all steps one by one for BeringMedia Ingestion Process
#   Author(s)   : DataMetica Team
#   Usage       : sh executor_patch.sh 
#   Date        : 04/10/2016
#   Log File    : .../log/ddp/EXECUTOR_patch.log
#   SQL File    : 
#   Error File  : .../log/ddp/EXECUTOR.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          04/10/2016       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

# Find absolute path to this script which is used to define module, project, subject area home directory paths.
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null

SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $HOME/etc/namespace.properties
source $HOME/etc/beeline.properties
source $HOME/etc/default.env.properties
source $HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/ddp.properties
source $SUBJECT_AREA_HOME/etc/oracle.properties

source $HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/ddp_functions.sh

##############################################################################
# Local Params				 								 	 			 #
##############################################################################

executor_log_file="${LOG_DIR}/EXECUTOR_PATCH.log"

##############################################################################
#																			 #
# Load data to incoming auxcust table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh auxcust medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming auxcust table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming auxcust table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming auxhouse table                                       #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh auxhouse medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming auxhouse table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming auxhouse table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming boxinvtry table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh boxinvtry large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming boxinvtry table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming boxinvtry table" "${executor_log_file}" 

##############################################################################
#																			 #
# Load data to incoming code36 table                                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh code36 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming code36 table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming code36 table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming code95 table                                         #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh code95 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming code95 table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming code95 table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming code999 table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh code999 medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming code999 table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming code999 table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custbilladdr table                                   #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh custbilladdr medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?

if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming custbilladdr table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming custbilladdr table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custmaster table                                     #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh custmaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming custmaster table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming custmaster table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming custrates table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh custrates large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming custrates table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming custrates table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming housemaster table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh housemaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming housemaster table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming housemaster table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming ratecodes table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh ratecodes medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming ratecodes table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming ratecodes table" "${executor_log_file}" 

##############################################################################
#																			 #
# Load data to incoming ratepricearea table                                  #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh ratepricearea medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming ratepricearea table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming ratepricearea table" "${executor_log_file}" 

##############################################################################
#																			 #
# Load data to incoming ratepricelevel table                                 #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh ratepricelevel medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming ratepricelevel table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming ratepricelevel table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming raterptctrs table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh raterptctrs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming raterptctrs table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming raterptctrs table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming rptctrs table                                        #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh rptctrs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming rptctrs table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming rptctrs table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming slsmn table                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh slsmn medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming slsmn table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming slsmn table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming techs table                                          #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh techs medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming techs table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming techs table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipcustrate table                                    #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh wipcustrate large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming wipcustrate table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming wipcustrate table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipmaster table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh wipmaster large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming wipmaster table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming wipmaster table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming wipoutlet table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh wipoutlet large 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming wipoutlet table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming wipoutlet table" "${executor_log_file}"

##############################################################################
#																			 #
# Load data to incoming zipmaster table                                      #
#																			 #
##############################################################################

sh $SUBJECT_AREA_HOME/bin/load_incoming_modules_patch.sh zipmaster medium 1>> ${executor_log_file} 2>> ${executor_log_file}
exit_code=$?
 
if [ ${exit_code} -ne '0' ]
then
   fn_log_error "Failed while loading data to incoming zipmaster table" "${executor_log_file}"
fi
fn_log_info "Successfully loaded data to incoming zipmaster table" "${executor_log_file}"

##############################################################################
#                                    End                                     #
##############################################################################
