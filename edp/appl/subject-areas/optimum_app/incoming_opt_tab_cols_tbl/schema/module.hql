--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Opt_tab_cols table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 09/12/2016
--#   Log File    : .../log/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          09/12/2016       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
    SUITE_NAME 	      VARCHAR(100) COMMENT '',
    HEADER1           VARCHAR(100) COMMENT '',
    HEADER2           VARCHAR(100) COMMENT '',
    HEADER3           VARCHAR(100) COMMENT '',
    HEADER4           VARCHAR(100) COMMENT '',
    HEADER5           VARCHAR(100) COMMENT '',
    HEADER6           VARCHAR(100) COMMENT '',
    HEADER7           VARCHAR(100) COMMENT '',
    HEADER8           VARCHAR(100) COMMENT '',
    HEADER9           VARCHAR(100) COMMENT '',
    HEADER10          VARCHAR(100) COMMENT '',
    HEADER11          VARCHAR(100) COMMENT '',
    HEADER12          VARCHAR(100) COMMENT '',
    HEADER13          VARCHAR(100) COMMENT '',
    HEADER14          VARCHAR(100) COMMENT '',
    HEADER15          VARCHAR(100) COMMENT '',
    HEADER16          VARCHAR(100) COMMENT '',
    HEADER17          VARCHAR(100) COMMENT '',
    HEADER18          VARCHAR(100) COMMENT '',
    HEADER19          VARCHAR(100) COMMENT '',
    HEADER20          VARCHAR(100) COMMENT '',
    HEADER21          VARCHAR(100) COMMENT '',
    HEADER22          VARCHAR(100) COMMENT '',
    HEADER23          VARCHAR(100) COMMENT '',
    HEADER24          VARCHAR(100) COMMENT '',
    HEADER25          VARCHAR(100) COMMENT '',
    HEADER26          VARCHAR(100) COMMENT '',
    HEADER27          VARCHAR(100) COMMENT '',
    HEADER28          VARCHAR(100) COMMENT '',
    HEADER29          VARCHAR(100) COMMENT '',
    HEADER30          VARCHAR(100) COMMENT '',
    HEADER31          VARCHAR(100) COMMENT '',
    HEADER32          VARCHAR(100) COMMENT '',
    HEADER33          VARCHAR(100) COMMENT '',
    HEADER34          VARCHAR(100) COMMENT '',
    HEADER35          VARCHAR(100) COMMENT '',
    HEADER36          VARCHAR(100) COMMENT '',
    HEADER37          VARCHAR(100) COMMENT '',
    HEADER38          VARCHAR(100) COMMENT '',
    HEADER39          VARCHAR(100) COMMENT '',
    HEADER40          VARCHAR(100) COMMENT '',
    HEADER41          VARCHAR(100) COMMENT '',
    HEADER42          VARCHAR(100) COMMENT '',
    HEADER43          VARCHAR(100) COMMENT '',
    HEADER44          VARCHAR(100) COMMENT '',
    HEADER45          VARCHAR(100) COMMENT '',
    HEADER46          VARCHAR(100) COMMENT '',
    HEADER47          VARCHAR(100) COMMENT '',
    HEADER48          VARCHAR(100) COMMENT '',
    HEADER49          VARCHAR(100) COMMENT '',
    HEADER50          VARCHAR(100) COMMENT '',
    HEADER51          VARCHAR(100) COMMENT '',
    HEADER52          VARCHAR(100) COMMENT '',
    HEADER53          VARCHAR(100) COMMENT '',
    HEADER54          VARCHAR(100) COMMENT '',
    HEADER55          VARCHAR(100) COMMENT '',
    HEADER56          VARCHAR(100) COMMENT '',
    HEADER57          VARCHAR(100) COMMENT '',
    HEADER58          VARCHAR(100) COMMENT '',
    HEADER59          VARCHAR(100) COMMENT '',
    HEADER60          VARCHAR(100) COMMENT '',
    HEADER61          VARCHAR(100) COMMENT '',
    HEADER62          VARCHAR(100) COMMENT '',
    HEADER63          VARCHAR(100) COMMENT '',
    HEADER64          VARCHAR(100) COMMENT '',
    HEADER65          VARCHAR(100) COMMENT '',
    HEADER66          VARCHAR(100) COMMENT '',
    HEADER67          VARCHAR(100) COMMENT '',
    HEADER68          VARCHAR(100) COMMENT '',
    HEADER69          VARCHAR(100) COMMENT '',
    HEADER70          VARCHAR(100) COMMENT '',
    HEADER71          VARCHAR(100) COMMENT '',
    HEADER72          VARCHAR(100) COMMENT '',
    HEADER73          VARCHAR(100) COMMENT '',
    HEADER74          VARCHAR(100) COMMENT '',
    HEADER75          VARCHAR(100) COMMENT '',
    HEADER76          VARCHAR(100) COMMENT '',
    HEADER77          VARCHAR(100) COMMENT '',
    HEADER78          VARCHAR(100) COMMENT '',
    HEADER79          VARCHAR(100) COMMENT '',
    HEADER80          VARCHAR(100) COMMENT '',
    HEADER81          VARCHAR(100) COMMENT '',
    HEADER82          VARCHAR(100) COMMENT '',
    HEADER83          VARCHAR(100) COMMENT '',
    HEADER84          VARCHAR(100) COMMENT '',
    HEADER85          VARCHAR(100) COMMENT '',
    HEADER86          VARCHAR(100) COMMENT '',
    HEADER87          VARCHAR(100) COMMENT '',
    HEADER88          VARCHAR(100) COMMENT '',
    HEADER89          VARCHAR(100) COMMENT '',
    HEADER90          VARCHAR(100) COMMENT '',
    HEADER91          VARCHAR(100) COMMENT '',
    HEADER92          VARCHAR(100) COMMENT '',
    HEADER93          VARCHAR(100) COMMENT '',
    HEADER94          VARCHAR(100) COMMENT '',
    HEADER95          VARCHAR(100) COMMENT '',
    HEADER96          VARCHAR(100) COMMENT '',
    HEADER97          VARCHAR(100) COMMENT '',
    HEADER98          VARCHAR(100) COMMENT '',
    HEADER99          VARCHAR(100) COMMENT '',
    HEADER100         VARCHAR(100) COMMENT '',
    HEADER101         VARCHAR(100) COMMENT '',
    HEADER102         VARCHAR(100) COMMENT '',
    HEADER103         VARCHAR(100) COMMENT '',
    HEADER104         VARCHAR(100) COMMENT '',
    HEADER105         VARCHAR(100) COMMENT '',
    HEADER106         VARCHAR(100) COMMENT '',
    HEADER107         VARCHAR(100) COMMENT '',
    HEADER108         VARCHAR(100) COMMENT '',
    HEADER109         VARCHAR(100) COMMENT '',
    HEADER110         VARCHAR(100) COMMENT '',
    HEADER111         VARCHAR(100) COMMENT '',
    HEADER112         VARCHAR(100) COMMENT '',
    HEADER113         VARCHAR(100) COMMENT '',
    HEADER114         VARCHAR(100) COMMENT '',
    HEADER115         VARCHAR(100) COMMENT '',
    HEADER116         VARCHAR(100) COMMENT '',
    HEADER117         VARCHAR(100) COMMENT '',
    HEADER118         VARCHAR(100) COMMENT '',
    HEADER119         VARCHAR(100) COMMENT '',
    HEADER120         VARCHAR(100) COMMENT '',
    HEADER121         VARCHAR(100) COMMENT '',
    HEADER122         VARCHAR(100) COMMENT '',
    HEADER123         VARCHAR(100) COMMENT '',
    HEADER124         VARCHAR(100) COMMENT '',
    HEADER125         VARCHAR(100) COMMENT '',
    HEADER126         VARCHAR(100) COMMENT '',
    HEADER127         VARCHAR(100) COMMENT '',
    HEADER128         VARCHAR(100) COMMENT '',
    HEADER129         VARCHAR(100) COMMENT '',
    HEADER130         VARCHAR(100) COMMENT '',
    HEADER131         VARCHAR(100) COMMENT '',
    HEADER132         VARCHAR(100) COMMENT '',
    HEADER133         VARCHAR(100) COMMENT '',
    HEADER134         VARCHAR(100) COMMENT '',
    HEADER135         VARCHAR(100) COMMENT '',
    HEADER136         VARCHAR(100) COMMENT '',
    HEADER137         VARCHAR(100) COMMENT '',
    HEADER138         VARCHAR(100) COMMENT '',
    HEADER139         VARCHAR(100) COMMENT '',
    HEADER140         VARCHAR(100) COMMENT '',
    HEADER141         VARCHAR(100) COMMENT '',
    HEADER142         VARCHAR(100) COMMENT '',
    HEADER143         VARCHAR(100) COMMENT '',
    HEADER144         VARCHAR(100) COMMENT '',
    HEADER145         VARCHAR(100) COMMENT '',
    HEADER146         VARCHAR(100) COMMENT '',
    HEADER147         VARCHAR(100) COMMENT '',
    HEADER148         VARCHAR(100) COMMENT '',
    HEADER149         VARCHAR(100) COMMENT '',
    HEADER150         VARCHAR(100) COMMENT '',
    HEADER151         VARCHAR(100) COMMENT '',
    HEADER152         VARCHAR(100) COMMENT '',
    HEADER153         VARCHAR(100) COMMENT '',
    HEADER154         VARCHAR(100) COMMENT '',
    HEADER155         VARCHAR(100) COMMENT '',
    HEADER156         VARCHAR(100) COMMENT '',
    HEADER157         VARCHAR(100) COMMENT '',
    HEADER158         VARCHAR(100) COMMENT '',
    HEADER159         VARCHAR(100) COMMENT '',
    HEADER160         VARCHAR(100) COMMENT '',
    HEADER161         VARCHAR(100) COMMENT '',
    HEADER162         VARCHAR(100) COMMENT '',
    HEADER163         VARCHAR(100) COMMENT '',
    HEADER164         VARCHAR(100) COMMENT '',
    HEADER165         VARCHAR(100) COMMENT '',
    HEADER166         VARCHAR(100) COMMENT '',
    HEADER167         VARCHAR(100) COMMENT '',
    HEADER168         VARCHAR(100) COMMENT '',
    HEADER169         VARCHAR(100) COMMENT '',
    HEADER170         VARCHAR(100) COMMENT '',
    HEADER171         VARCHAR(100) COMMENT '',
    HEADER172         VARCHAR(100) COMMENT '',
    HEADER173         VARCHAR(100) COMMENT '',
    HEADER174         VARCHAR(100) COMMENT '',
    HEADER175         VARCHAR(100) COMMENT '',
    HEADER176         VARCHAR(100) COMMENT '',
    HEADER177         VARCHAR(100) COMMENT '',
    HEADER178         VARCHAR(100) COMMENT '',
    HEADER179         VARCHAR(100) COMMENT '',
    HEADER180         VARCHAR(100) COMMENT '',
    HEADER181         VARCHAR(100) COMMENT '',
    HEADER182         VARCHAR(100) COMMENT '',
    HEADER183         VARCHAR(100) COMMENT '',
    HEADER184         VARCHAR(100) COMMENT '',
    HEADER185         VARCHAR(100) COMMENT '',
    HEADER186         VARCHAR(100) COMMENT '',
    HEADER187         VARCHAR(100) COMMENT '',
    HEADER188         VARCHAR(100) COMMENT '',
    HEADER189         VARCHAR(100) COMMENT '',
    HEADER190         VARCHAR(100) COMMENT '',
    HEADER191         VARCHAR(100) COMMENT '',
    HEADER192         VARCHAR(100) COMMENT '',
    HEADER193         VARCHAR(100) COMMENT '',
    HEADER194         VARCHAR(100) COMMENT '',
    HEADER195         VARCHAR(100) COMMENT '',
    HEADER196         VARCHAR(100) COMMENT '',
    HEADER197         VARCHAR(100) COMMENT '',
    HEADER198         VARCHAR(100) COMMENT '',
    HEADER199         VARCHAR(100) COMMENT '',
    HEADER200         VARCHAR(100) COMMENT '',
    HEADER201         VARCHAR(100) COMMENT '',
    HEADER202         VARCHAR(100) COMMENT '',
    HEADER203         VARCHAR(100) COMMENT '',
    HEADER204         VARCHAR(100) COMMENT '',
    HEADER205         VARCHAR(100) COMMENT '',
    HEADER206         VARCHAR(100) COMMENT '',
    HEADER207         VARCHAR(100) COMMENT '',
    HEADER208         VARCHAR(100) COMMENT '',
    HEADER209         VARCHAR(100) COMMENT '',
    HEADER210         VARCHAR(100) COMMENT '',
    HEADER211         VARCHAR(100) COMMENT '',
    HEADER212         VARCHAR(100) COMMENT '',
    HEADER213         VARCHAR(100) COMMENT '',
    HEADER214         VARCHAR(100) COMMENT '',
    HEADER215         VARCHAR(100) COMMENT '',
    HEADER216         VARCHAR(100) COMMENT '',
    HEADER217         VARCHAR(100) COMMENT '',
    HEADER218         VARCHAR(100) COMMENT '',
    HEADER219         VARCHAR(100) COMMENT '',
    HEADER220         VARCHAR(100) COMMENT '',
    HEADER221         VARCHAR(100) COMMENT '',
    HEADER222         VARCHAR(100) COMMENT '',
    HEADER223         VARCHAR(100) COMMENT '',
    HEADER224         VARCHAR(100) COMMENT '',
    HEADER225         VARCHAR(100) COMMENT '',
    HEADER226         VARCHAR(100) COMMENT '',
    HEADER227         VARCHAR(100) COMMENT '',
    HEADER228         VARCHAR(100) COMMENT '',
    HEADER229         VARCHAR(100) COMMENT '',
    HEADER230         VARCHAR(100) COMMENT '',
    HEADER231         VARCHAR(100) COMMENT '',
    HEADER232         VARCHAR(100) COMMENT '',
    HEADER233         VARCHAR(100) COMMENT '',
    HEADER234         VARCHAR(100) COMMENT '',
    HEADER235         VARCHAR(100) COMMENT '',
    HEADER236         VARCHAR(100) COMMENT '',
    HEADER237         VARCHAR(100) COMMENT '',
    HEADER238         VARCHAR(100) COMMENT '',
    HEADER239         VARCHAR(100) COMMENT '',
    HEADER240         VARCHAR(100) COMMENT '',
    HEADER241         VARCHAR(100) COMMENT '',
    HEADER242         VARCHAR(100) COMMENT '',
    HEADER243         VARCHAR(100) COMMENT '',
    HEADER244         VARCHAR(100) COMMENT '',
    HEADER245         VARCHAR(100) COMMENT '',
    HEADER246         VARCHAR(100) COMMENT '',
    HEADER247         VARCHAR(100) COMMENT '',
    HEADER248         VARCHAR(100) COMMENT '',
    HEADER249         VARCHAR(100) COMMENT '',
    HEADER250         VARCHAR(100) COMMENT '',
    HEADER251         VARCHAR(100) COMMENT '',
    HEADER252         VARCHAR(100) COMMENT '',
    HEADER253         VARCHAR(100) COMMENT '',
    HEADER254         VARCHAR(100) COMMENT '',
    HEADER255         VARCHAR(100) COMMENT '',
    HEADER256         VARCHAR(100) COMMENT '',
    HEADER257         VARCHAR(100) COMMENT '',
    HEADER258         VARCHAR(100) COMMENT '',
    HEADER259         VARCHAR(100) COMMENT '',
    HEADER260         VARCHAR(100) COMMENT '',
    HEADER261         VARCHAR(100) COMMENT '',
    HEADER262         VARCHAR(100) COMMENT '',
    HEADER263         VARCHAR(100) COMMENT '',
    HEADER264         VARCHAR(100) COMMENT '',
    HEADER265         VARCHAR(100) COMMENT '',
    HEADER266         VARCHAR(100) COMMENT '',
    HEADER267         VARCHAR(100) COMMENT '',
    HEADER268         VARCHAR(100) COMMENT '',
    HEADER269         VARCHAR(100) COMMENT '',
    HEADER270         VARCHAR(100) COMMENT '',
    HEADER271         VARCHAR(100) COMMENT '',
    HEADER272         VARCHAR(100) COMMENT '',
    HEADER273         VARCHAR(100) COMMENT '',
    HEADER274         VARCHAR(100) COMMENT '',
    HEADER275         VARCHAR(100) COMMENT '',
    HEADER276         VARCHAR(100) COMMENT '',
    HEADER277         VARCHAR(100) COMMENT '',
    HEADER278         VARCHAR(100) COMMENT '',
    HEADER279         VARCHAR(100) COMMENT '',
    HEADER280         VARCHAR(100) COMMENT '',
    HEADER281         VARCHAR(100) COMMENT '',
    HEADER282         VARCHAR(100) COMMENT '',
    HEADER283         VARCHAR(100) COMMENT '',
    HEADER284         VARCHAR(100) COMMENT '',
    HEADER285         VARCHAR(100) COMMENT '',
    HEADER286         VARCHAR(100) COMMENT '',
    HEADER287         VARCHAR(100) COMMENT '',
    HEADER288         VARCHAR(100) COMMENT '',
    HEADER289         VARCHAR(100) COMMENT '',
    HEADER290         VARCHAR(100) COMMENT '',
    HEADER291         VARCHAR(100) COMMENT '',
    HEADER292         VARCHAR(100) COMMENT '',
    HEADER293         VARCHAR(100) COMMENT '',
    HEADER294         VARCHAR(100) COMMENT '',
    HEADER295         VARCHAR(100) COMMENT '',
    HEADER296         VARCHAR(100) COMMENT '',
    HEADER297         VARCHAR(100) COMMENT '',
    HEADER298         VARCHAR(100) COMMENT '',
    HEADER299         VARCHAR(100) COMMENT '',
    HEADER300         VARCHAR(100) COMMENT '',
    HEADER301         VARCHAR(100) COMMENT '',
    HEADER302         VARCHAR(100) COMMENT '',
    HEADER303         VARCHAR(100) COMMENT '',
    HEADER304         VARCHAR(100) COMMENT '',
    HEADER305         VARCHAR(100) COMMENT '',
    HEADER306         VARCHAR(100) COMMENT '',
    HEADER307         VARCHAR(100) COMMENT '',
    HEADER308         VARCHAR(100) COMMENT '',
    HEADER309         VARCHAR(100) COMMENT '',
    HEADER310         VARCHAR(100) COMMENT '',
    HEADER311         VARCHAR(100) COMMENT '',
    HEADER312         VARCHAR(100) COMMENT '',
    HEADER313         VARCHAR(100) COMMENT '',
    HEADER314         VARCHAR(100) COMMENT '',
    HEADER315         VARCHAR(100) COMMENT '',
    HEADER316         VARCHAR(100) COMMENT '',
    HEADER317         VARCHAR(100) COMMENT '',
    HEADER318         VARCHAR(100) COMMENT '',
    HEADER319         VARCHAR(100) COMMENT '',
    HEADER320         VARCHAR(100) COMMENT '',
    HEADER321         VARCHAR(100) COMMENT '',
    HEADER322         VARCHAR(100) COMMENT '',
    HEADER323         VARCHAR(100) COMMENT '',
    HEADER324         VARCHAR(100) COMMENT '',
    HEADER325         VARCHAR(100) COMMENT '',
    HEADER326         VARCHAR(100) COMMENT '',
    HEADER327         VARCHAR(100) COMMENT '',
    HEADER328         VARCHAR(100) COMMENT '',
    HEADER329         VARCHAR(100) COMMENT '',
    HEADER330         VARCHAR(100) COMMENT '',
    HEADER331         VARCHAR(100) COMMENT '',
    HEADER332         VARCHAR(100) COMMENT '',
    HEADER333         VARCHAR(100) COMMENT '',
    HEADER334         VARCHAR(100) COMMENT '',
    HEADER335         VARCHAR(100) COMMENT '',
    HEADER336         VARCHAR(100) COMMENT '',
    HEADER337         VARCHAR(100) COMMENT '',
    HEADER338         VARCHAR(100) COMMENT '',
    HEADER339         VARCHAR(100) COMMENT '',
    HEADER340         VARCHAR(100) COMMENT '',
    HEADER341         VARCHAR(100) COMMENT '',
    HEADER342         VARCHAR(100) COMMENT '',
    HEADER343         VARCHAR(100) COMMENT '',
    HEADER344         VARCHAR(100) COMMENT '',
    HEADER345         VARCHAR(100) COMMENT '',
    HEADER346         VARCHAR(100) COMMENT '',
    HEADER347         VARCHAR(100) COMMENT '',
    HEADER348         VARCHAR(100) COMMENT '',
    HEADER349         VARCHAR(100) COMMENT '',
    HEADER350         VARCHAR(100) COMMENT '',
    HEADER351         VARCHAR(100) COMMENT '',
    HEADER352         VARCHAR(100) COMMENT '',
    HEADER353         VARCHAR(100) COMMENT '',
    HEADER354         VARCHAR(100) COMMENT '',
    HEADER355         VARCHAR(100) COMMENT '',
    HEADER356         VARCHAR(100) COMMENT '',
    HEADER357         VARCHAR(100) COMMENT '',
    HEADER358         VARCHAR(100) COMMENT '',
    HEADER359         VARCHAR(100) COMMENT '',
    HEADER360          VARCHAR(100) COMMENT '',
    HEADER361          VARCHAR(100) COMMENT '',
    HEADER362          VARCHAR(100) COMMENT '',
    HEADER363          VARCHAR(100) COMMENT '',
    HEADER364          VARCHAR(100) COMMENT '',
    HEADER365          VARCHAR(100) COMMENT '',
    HEADER366          VARCHAR(100) COMMENT '',
    HEADER367          VARCHAR(100) COMMENT '',
    HEADER368          VARCHAR(100) COMMENT '',
    HEADER369          VARCHAR(100) COMMENT '',
    HEADER370          VARCHAR(100) COMMENT '',
    HEADER371          VARCHAR(100) COMMENT '',
    HEADER372          VARCHAR(100) COMMENT '',
    HEADER373          VARCHAR(100) COMMENT '',
    HEADER374          VARCHAR(100) COMMENT '',
    HEADER375          VARCHAR(100) COMMENT '',
    HEADER376          VARCHAR(100) COMMENT '',
    HEADER377          VARCHAR(100) COMMENT '',
    HEADER378          VARCHAR(100) COMMENT '',
    HEADER379          VARCHAR(100) COMMENT '',
    HEADER380          VARCHAR(100) COMMENT '',
    HEADER381          VARCHAR(100) COMMENT '',
    HEADER382          VARCHAR(100) COMMENT '',
    HEADER383          VARCHAR(100) COMMENT '',
    HEADER384          VARCHAR(100) COMMENT '',
    HEADER385          VARCHAR(100) COMMENT '',
    HEADER386          VARCHAR(100) COMMENT '',
    HEADER387          VARCHAR(100) COMMENT '',
    HEADER388          VARCHAR(100) COMMENT '',
    HEADER389          VARCHAR(100) COMMENT '',
    HEADER390          VARCHAR(100) COMMENT '',
    HEADER391          VARCHAR(100) COMMENT '',
    HEADER392          VARCHAR(100) COMMENT '',
    HEADER393          VARCHAR(100) COMMENT '',
    HEADER394          VARCHAR(100) COMMENT '',
    HEADER395          VARCHAR(100) COMMENT '',
    HEADER396          VARCHAR(100) COMMENT '',
    HEADER397          VARCHAR(100) COMMENT '',
    HEADER398          VARCHAR(100) COMMENT '',
    HEADER399          VARCHAR(100) COMMENT '',
    HEADER400          VARCHAR(100) COMMENT '',
    HEADER401          VARCHAR(100) COMMENT '',
    HEADER402          VARCHAR(100) COMMENT '',
    HEADER403          VARCHAR(100) COMMENT '',
    HEADER404          VARCHAR(100) COMMENT '',
    HEADER405          VARCHAR(100) COMMENT '',
    HEADER406          VARCHAR(100) COMMENT '',
    HEADER407          VARCHAR(100) COMMENT '',
    HEADER408          VARCHAR(100) COMMENT '',
    HEADER409          VARCHAR(100) COMMENT '',
    HEADER410          VARCHAR(100) COMMENT '',
    HEADER411          VARCHAR(100) COMMENT '',
    HEADER412          VARCHAR(100) COMMENT '',
    HEADER413          VARCHAR(100) COMMENT '',
    HEADER414          VARCHAR(100) COMMENT '',
    HEADER415          VARCHAR(100) COMMENT '',
    HEADER416          VARCHAR(100) COMMENT '',
    HEADER417          VARCHAR(100) COMMENT '',
    HEADER418          VARCHAR(100) COMMENT '',
    HEADER419          VARCHAR(100) COMMENT '',
    HEADER420          VARCHAR(100) COMMENT '',
    HEADER421          VARCHAR(100) COMMENT '',
    HEADER422          VARCHAR(100) COMMENT '',
    HEADER423          VARCHAR(100) COMMENT '',
    HEADER424          VARCHAR(100) COMMENT '',
    HEADER425          VARCHAR(100) COMMENT '',
    HEADER426          VARCHAR(100) COMMENT '',
    HEADER427          VARCHAR(100) COMMENT '',
    HEADER428          VARCHAR(100) COMMENT '',
    HEADER429          VARCHAR(100) COMMENT '',
    HEADER430          VARCHAR(100) COMMENT '',
    HEADER431          VARCHAR(100) COMMENT '',
    HEADER432          VARCHAR(100) COMMENT '',
    HEADER433          VARCHAR(100) COMMENT '',
    HEADER434          VARCHAR(100) COMMENT '',
    HEADER435          VARCHAR(100) COMMENT '',
    HEADER436          VARCHAR(100) COMMENT '',
    HEADER437          VARCHAR(100) COMMENT '',
    HEADER438          VARCHAR(100) COMMENT '',
    HEADER439          VARCHAR(100) COMMENT '',
    HEADER440          VARCHAR(100) COMMENT '',
    HEADER441          VARCHAR(100) COMMENT '',
    HEADER442          VARCHAR(100) COMMENT '',
    HEADER443          VARCHAR(100) COMMENT '',
    HEADER444          VARCHAR(100) COMMENT '',
    HEADER445          VARCHAR(100) COMMENT '',
    HEADER446          VARCHAR(100) COMMENT '',
    HEADER447          VARCHAR(100) COMMENT '',
    HEADER448          VARCHAR(100) COMMENT '',
    HEADER449          VARCHAR(100) COMMENT '',
    HEADER450          VARCHAR(100) COMMENT '',
    HEADER451          VARCHAR(100) COMMENT '',
    HEADER452          VARCHAR(100) COMMENT '',
    HEADER453          VARCHAR(100) COMMENT '',
    HEADER454          VARCHAR(100) COMMENT '',
    HEADER455          VARCHAR(100) COMMENT '',
    HEADER456          VARCHAR(100) COMMENT '',
    HEADER457          VARCHAR(100) COMMENT '',
    HEADER458          VARCHAR(100) COMMENT '',
    HEADER459          VARCHAR(100) COMMENT '',
    HEADER460          VARCHAR(100) COMMENT '',
    HEADER461          VARCHAR(100) COMMENT '',
    HEADER462          VARCHAR(100) COMMENT '',
    HEADER463          VARCHAR(100) COMMENT '',
    HEADER464          VARCHAR(100) COMMENT '',
    HEADER465          VARCHAR(100) COMMENT '',
    HEADER466          VARCHAR(100) COMMENT '',
    HEADER467          VARCHAR(100) COMMENT '',
    HEADER468          VARCHAR(100) COMMENT '',
    HEADER469          VARCHAR(100) COMMENT '',
    HEADER470          VARCHAR(100) COMMENT '',
    HEADER471          VARCHAR(100) COMMENT '',
    HEADER472          VARCHAR(100) COMMENT '',
    HEADER473          VARCHAR(100) COMMENT '',
    HEADER474          VARCHAR(100) COMMENT '',
    HEADER475          VARCHAR(100) COMMENT '',
    HEADER476          VARCHAR(100) COMMENT ''
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################