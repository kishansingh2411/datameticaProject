var app =  angular.module("webApp",['ngRoute','ngAnimate']);

//Add Date Picker On Selection Here
//No Dydefault Chart should be shown
//Onclick  userSegment Chart on Click By Country

/*New 

http://159.253.128.86:8080/

*/

/*Dev URl
http://10.200.99.47:8080/

*/
var services =
{
common1 :  "http://159.253.128.86:8080/data-service/clickstream/data?",
common : "http://10.200.99.47:8080/data-service/clickstream/data?",


//commonkpi : "http://159.253.128.86:8080/data-service/clickstream/data?",
block1st : "dimensions=date&metrics=totalRevenue,totalOrders,totalPageViews,cartAbandonment",
block2nd : "dimensions=country,week,visitorSegment&metrics=totalVisits,totalViewedProducts,shoppingCartStarted,totalOrders",
kpi1 : "dimensions=week,country,visitorSegment&metrics=totalVisits,totalOrders",
kpi2 : "dimensions=week,country,visitorSegment&metrics=totalRevenue,totalOrders",
kpi3 : "dimensions=week,country,visitorSegment&metrics=totalPageViews,totalVisits",
kpi4a : "dimensions=week,country,visitorSegment&metrics=totalRevenueFromNewCustomers,totalRevenue",
kpi4b : "dimensions=week,country,visitorSegment&metrics=totalRevenueFromExistingCustomers,totalRevenue",
kpi5 : "dimensions=week,country,visitorSegment&metrics=totalOrders,shoppingCartStarted",



ocr : "dimensions=date,country&metrics=totalVisits,totalOrders",
ocrCharts : "dimensions=date,country,visitorSegment&metrics=totalVisits,totalOrders",
dataFrom  :"&fromDate=",
dataTo   :  "&toDate=",
fromWeek : "&fromDate=", 
toWeek : "&toDate=", 

/*Average Revenue Per Order*/
avgRpo : "dimensions=date,country&metrics=totalRevenue,totalOrders",
avgRpoCharts : "dimensions=date,country,visitorSegment&metrics=totalRevenue,totalOrders",

/*Average Page View Per Visit*/
avgPVPV : "dimensions=date,country&metrics=totalPageViews,totalVisits",
avgPVPVCharts : "dimensions=date,country,visitorSegment&metrics=totalPageViews,totalVisits",


/*Percent Revenue From New Customers*/
perRFNC : "dimensions=date,country&metrics=totalRevenueFromNewCustomers,totalRevenue",
perRFNCCharts : "dimensions=date,country,visitorSegment&metrics=totalRevenueFromNewCustomers,totalRevenue",

/*Percent Revenue From Exisitng Customers*/
perRFEC  :"dimensions=date,country&metrics=totalRevenueFromExistingCustomers,totalRevenue",
perRFECCharts :"dimensions=date,country,visitorSegment&metrics=totalRevenueFromExistingCustomers,totalRevenue",

/*Shopping Cart Compilation Rate*/
shopCart : "dimensions=date,country&metrics=totalOrders,shoppingCartStarted",
shopCartCharts : "dimensions=date,country,visitorSegment&metrics=totalOrders,shoppingCartStarted",


/*Below Block Are URL for Common Data For View Check 1stBlock For Reference*/
block1stRevenue : "dimensions=date,country&metrics=totalRevenue",
block1stCartAban :"dimensions=date,country&metrics=cartAbandonment",
block1stTotalOrders : "dimensions=date,country&metrics=totalOrders",
block1stPageView : "dimensions=date,country&metrics=totalPageViews",
block1stAllDim :"dimensions=date,country&metrics=totalRevenue,cartAbandonment,totalOrders,totalPageViews",


block1Weekly : "dimensions=week,visitorSegment,country&metrics=totalRevenue,totalOrders,totalPageViews,cartAbandonment",
block2Weekly : "dimensions=week,country&metrics=totalVisits,totalViewedProducts,shoppingCartStarted,totalOrders"


}
/*

OCR BACKUP
ocr:"dimensions=date,country,visitorSegment&metrics=totalVisits,totalOrders"
*/

// local Data Incase AJAX FAILS TO LOAD

var block1st =
{"query":{"dimensions":"date","metrics":"totalRevenue,totalOrder,totalPageViews,cartAbandonment","fromDate":"2014-04-02","toDate":"2014-04-03","limit":100},"columnHeaders":[{"name":"date","type":"DIMENSION","dataType":"DATE"},{"name":"totalRevenue","type":"METRIC","dataType":null},{"name":"totalOrder","type":"METRIC","dataType":"LONG"},{"name":"totalPageViews","type":"METRIC","dataType":"LONG"},{"name":"cartAbandonment","type":"METRIC","dataType":"LONG"}],"rows":[["2014-04-02","4333290.000000","577720","288860","15990"],["2014-04-03","4333290.000000","577720","288860","15990"]]
};


var block2nd =
{"query":{"dimensions":"country","metrics":"totalVisits,totalViewedProducts,shoppingCartStarted,totalOrders","fromDate":"2014-04-02","toDate":"2014-05-30","limit":100},"columnHeaders":[{"name":"country","type":"DIMENSION","dataType":"STRING"},{"name":"totalVisits","type":"METRIC","dataType":"LONG"},{"name":"totalViewedProducts","type":"METRIC","dataType":"LONG"},{"name":"shoppingCartStarted","type":"METRIC","dataType":"LONG"},{"name":"totalOrders","type":"METRIC","dataType":"LONG"}],"rows":[["AUS","32190","966570","1610950","35670"],["BUL","32190","966570","1610950","35670"],["FIN","32190","966570","1610950","35670"],["FRN","32190","966570","1610950","35670"],["GER","32190","966570","1610950","35670"],["HUN","32190","966570","1610950","35670"],["IND","32190","966570","1610950","35670"],["NDR","32190","966570","1610950","35670"],["POL","32190","966570","1610950","35670"],["SER","32190","966570","1610950","35670"],["UK","32190","966570","1610950","35670"],["UKR","32190","966570","1610950","35670"],["US","32190","966570","1610950","35670"]]
};


var ocr =
{"query":{"dimensions":"date,country,visitorSegment","metrics":"totalVisits,totalOrders","fromDate":"2014-04-02","toDate":"2014-05-30","limit":100},"columnHeaders":[{"name":"date","type":"DIMENSION","dataType":"DATE"},{"name":"country","type":"DIMENSION","dataType":"STRING"},{"name":"visitorSegment","type":"DIMENSION","dataType":"STRING"},{"name":"totalVisits","type":"METRIC","dataType":"LONG"},{"name":"totalOrders","type":"METRIC","dataType":"LONG"}],"rows":[["2014-04-02","AUS","A2B","111","123"],["2014-04-02","AUS","A2D","111","123"],["2014-04-02","AUS","A2E","111","123"],["2014-04-02","AUS","A2F","111","123"],["2014-04-02","AUS","B2A","111","123"],["2014-04-02","AUS","B2B","111","123"],["2014-04-02","AUS","B2C","111","123"],["2014-04-02","AUS","C2B","111","123"],["2014-04-02","AUS","D2A","111","123"],["2014-04-02","AUS","E2A","111","123"],["2014-04-02","BUL","A2B","111","123"],["2014-04-02","BUL","A2D","111","123"],["2014-04-02","BUL","A2E","111","123"],["2014-04-02","BUL","A2F","111","123"],["2014-04-02","BUL","B2A","111","123"],["2014-04-02","BUL","B2B","111","123"],["2014-04-02","BUL","B2C","111","123"],["2014-04-02","BUL","C2B","111","123"],["2014-04-02","BUL","D2A","111","123"],["2014-04-02","BUL","E2A","111","123"],["2014-04-02","FIN","A2B","111","123"],["2014-04-02","FIN","A2D","111","123"],["2014-04-02","FIN","A2E","111","123"],["2014-04-02","FIN","A2F","111","123"],["2014-04-02","FIN","B2A","111","123"],["2014-04-02","FIN","B2B","111","123"],["2014-04-02","FIN","B2C","111","123"],["2014-04-02","FIN","C2B","111","123"],["2014-04-02","FIN","D2A","111","123"],["2014-04-02","FIN","E2A","111","123"],["2014-04-02","FRN","A2B","111","123"],["2014-04-02","FRN","A2D","111","123"],["2014-04-02","FRN","A2E","111","123"],["2014-04-02","FRN","A2F","111","123"],["2014-04-02","FRN","B2A","111","123"],["2014-04-02","FRN","B2B","111","123"],["2014-04-02","FRN","B2C","111","123"],["2014-04-02","FRN","C2B","111","123"],["2014-04-02","FRN","D2A","111","123"],["2014-04-02","FRN","E2A","111","123"],["2014-04-02","GER","A2B","111","123"],["2014-04-02","GER","A2D","111","123"],["2014-04-02","GER","A2E","111","123"],["2014-04-02","GER","A2F","111","123"],["2014-04-02","GER","B2A","111","123"],["2014-04-02","GER","B2B","111","123"],["2014-04-02","GER","B2C","111","123"],["2014-04-02","GER","C2B","111","123"],["2014-04-02","GER","D2A","111","123"],["2014-04-02","GER","E2A","111","123"],["2014-04-02","HUN","A2B","111","123"],["2014-04-02","HUN","A2D","111","123"],["2014-04-02","HUN","A2E","111","123"],["2014-04-02","HUN","A2F","111","123"],["2014-04-02","HUN","B2A","111","123"],["2014-04-02","HUN","B2B","111","123"],["2014-04-02","HUN","B2C","111","123"],["2014-04-02","HUN","C2B","111","123"],["2014-04-02","HUN","D2A","111","123"],["2014-04-02","HUN","E2A","111","123"],["2014-04-02","IND","A2B","111","123"],["2014-04-02","IND","A2D","111","123"],["2014-04-02","IND","A2E","111","123"],["2014-04-02","IND","A2F","111","123"],["2014-04-02","IND","B2A","111","123"],["2014-04-02","IND","B2B","111","123"],["2014-04-02","IND","B2C","111","123"],["2014-04-02","IND","C2B","111","123"],["2014-04-02","IND","D2A","111","123"],["2014-04-02","IND","E2A","111","123"],["2014-04-02","NDR","A2B","111","123"],["2014-04-02","NDR","A2D","111","123"],["2014-04-02","NDR","A2E","111","123"],["2014-04-02","NDR","A2F","111","123"],["2014-04-02","NDR","B2A","111","123"],["2014-04-02","NDR","B2B","111","123"],["2014-04-02","NDR","B2C","111","123"],["2014-04-02","NDR","C2B","111","123"],["2014-04-02","NDR","D2A","111","123"],["2014-04-02","NDR","E2A","111","123"],["2014-04-02","POL","A2B","111","123"],["2014-04-02","POL","A2D","111","123"],["2014-04-02","POL","A2E","111","123"],["2014-04-02","POL","A2F","111","123"],["2014-04-02","POL","B2A","111","123"],["2014-04-02","POL","B2B","111","123"],["2014-04-02","POL","B2C","111","123"],["2014-04-02","POL","C2B","111","123"],["2014-04-02","POL","D2A","111","123"],["2014-04-02","POL","E2A","111","123"],["2014-04-02","SER","A2B","111","123"],["2014-04-02","SER","A2D","111","123"],["2014-04-02","SER","A2E","111","123"],["2014-04-02","SER","A2F","111","123"],["2014-04-02","SER","B2A","111","123"],["2014-04-02","SER","B2B","111","123"],["2014-04-02","SER","B2C","111","123"],["2014-04-02","SER","C2B","111","123"],["2014-04-02","SER","D2A","111","123"],["2014-04-02","SER","E2A","111","123"]]
};



var block1Weekly = {"query":{"dimensions":"week,visitorSegment","metrics":"totalRevenue,totalOrders,totalPageViews,cartAbandonment","fromDate":"2015W12","toDate":"2015W14"},"columnHeaders":[{"name":"week","type":"DIMENSION","dataType":"DATE"},{"name":"visitorSegment","type":"DIMENSION","dataType":"STRING"},{"name":"totalRevenue","type":"METRIC","dataType":null},{"name":"totalOrders","type":"METRIC","dataType":"LONG"},{"name":"totalPageViews","type":"METRIC","dataType":"LONG"},{"name":"cartAbandonment","type":"METRIC","dataType":"LONG"}],"rows":[["2015W12","ANONYMOUSCUSTOMER","4501107.000000","857350","12044799","22919273"],["2015W12","EXISTINGCUSTOMER","19852300.000000","854750","12030200","22925045"],["2015W12","NEWCUSTOMER","742300.000000","852150","12015601","22930817"],["2015W13","ANONYMOUSCUSTOMER","4705350.000000","890500","12614160","24027770"],["2015W13","EXISTINGCUSTOMER","20831577.000000","891150","12599561","23970037"],["2015W13","NEWCUSTOMER","769600.000000","891800","12584962","23912304"],["2015W14","ANONYMOUSCUSTOMER","4952844.000000","933400","13183521","24945752"],["2015W14","EXISTINGCUSTOMER","21840754.000000","930800","13168922","24951524"],["2015W14","NEWCUSTOMER","793000.000000","928200","13154323","24957296"]]};
var block2Weekly = {"query":{"dimensions":"week,country","metrics":"totalVisits,totalViewedProducts,shoppingCartStarted,totalOrders","fromDate":"2015W02","toDate":"2015W24"},"columnHeaders":[{"name":"week","type":"DIMENSION","dataType":"DATE"},{"name":"country","type":"DIMENSION","dataType":"STRING"},{"name":"totalVisits","type":"METRIC","dataType":"LONG"},{"name":"totalViewedProducts","type":"METRIC","dataType":"LONG"},{"name":"shoppingCartStarted","type":"METRIC","dataType":"LONG"},{"name":"totalOrders","type":"METRIC","dataType":"LONG"}],"rows":[["2015W02","AUSTRIA","1115664","505200","253200","179400"],["2015W02","BELGIUM","1115664","505200","253200","179400"],["2015W02","CZECH REPUBLIC","1115664","505200","253200","179400"],["2015W02","GERMANY","1115664","505200","253200","179400"],["2015W02","IRELAND","1115664","505200","253200","179400"],["2015W02","ITALY","1115664","505200","253200","179400"],["2015W02","NETHERLANDS","1115664","505200","253200","179400"],["2015W02","POLAND","1115664","505200","253200","179400"],["2015W02","SLOVENIA","1115664","505200","253200","179400"],["2015W02","SWITZERLAND","1115664","505200","253200","179400"],["2015W02","UNITED KINGDOM","1115664","505200","253200","179400"],["2015W03","AUSTRIA","1232880","500400","272400","174600"],["2015W03","BELGIUM","1243869","499950","274200","174150"],["2015W03","CZECH REPUBLIC","1177935","502650","263400","176850"],["2015W03","GERMANY","1144968","504000","258000","178200"],["2015W03","IRELAND","1221891","500850","270600","175050"],["2015W03","ITALY","1210902","501300","268800","175500"],["2015W03","NETHERLANDS","1133979","504450","256200","178650"],["2015W03","POLAND","1188924","502200","265200","176400"],["2015W03","SLOVENIA","1199913","501750","267000","175950"],["2015W03","SWITZERLAND","1254858","499500","276000","173700"],["2015W03","UNITED KINGDOM","1122990","504900","254400","179100"],["2015W04","AUSTRIA","1246809","547350","276600","192750"],["2015W04","BELGIUM","1245711","551850","276600","194550"],["2015W04","CZECH REPUBLIC","1252299","524850","276600","183750"],["2015W04","GERMANY","1255593","511350","276600","178350"],["2015W04","IRELAND","1247907","542850","276600","190950"],["2015W04","ITALY","1249005","538350","276600","189150"],["2015W04","NETHERLANDS","1256691","506850","276600","176550"],["2015W04","POLAND","1251201","529350","276600","185550"],["2015W04","SLOVENIA","1250103","533850","276600","187350"],["2015W04","SWITZERLAND","1244613","556350","276600","196350"],["2015W04","UNITED KINGDOM","1257789","502350","276600","174750"],["2015W05","AUSTRIA","1361463","553050","295800","192150"],["2015W05","BELGIUM","1372452","552600","297600","191700"],["2015W05","CZECH REPUBLIC","1306518","555300","286800","194400"],["2015W05","GERMANY","1273551","556650","281400","195750"],["2015W05","IRELAND","1350474","553500","294000","192600"],["2015W05","ITALY","1339485","553950","292200","193050"],["2015W05","NETHERLANDS","1262562","557100","279600","196200"],["2015W05","POLAND","1317507","554850","288600","193950"],["2015W05","SLOVENIA","1328496","554400","290400","193500"],["2015W05","SWITZERLAND","1383441","552150","299400","191250"],["2015W05","UNITED KINGDOM","1251573","557550","277800","196650"],["2015W06","AUSTRIA","1387104","552000","300000","191100"],["2015W06","BELGIUM","1387104","552000","300000","191100"],["2015W06","CZECH REPUBLIC","1387104","552000","300000","191100"],["2015W06","GERMANY","1387104","552000","300000","191100"],["2015W06","IRELAND","1387104","552000","300000","191100"],["2015W06","ITALY","1387104","552000","300000","191100"],["2015W06","NETHERLANDS","1387104","552000","300000","191100"],["2015W06","POLAND","1387104","552000","300000","191100"],["2015W06","SLOVENIA","1387104","552000","300000","191100"],["2015W06","SWITZERLAND","1387104","552000","300000","191100"],["2015W06","UNITED KINGDOM","1387104","552000","300000","191100"],["2015W07","AUSTRIA","1387104","552000","300000","191100"],["2015W07","BELGIUM","1387104","552000","300000","191100"],["2015W07","CZECH REPUBLIC","1387104","552000","300000","191100"],["2015W07","GERMANY","1387104","552000","300000","191100"],["2015W07","IRELAND","1387104","552000","300000","191100"],["2015W07","ITALY","1387104","552000","300000","191100"],["2015W07","NETHERLANDS","1387104","552000","300000","191100"],["2015W07","POLAND","1387104","552000","300000","191100"],["2015W07","SLOVENIA","1387104","552000","300000","191100"],["2015W07","SWITZERLAND","1387104","552000","300000","191100"],["2015W07","UNITED KINGDOM","1387104","552000","300000","191100"],["2015W08","AUSTRIA","1387104","552000","300000","191100"],["2015W08","BELGIUM","1387104","552000","300000","191100"],["2015W08","CZECH REPUBLIC","1387104","552000","300000","191100"],["2015W08","GERMANY","1387104","552000","300000","191100"],["2015W08","IRELAND","1387104","552000","300000","191100"],["2015W08","ITALY","1387104","552000","300000","191100"],["2015W08","NETHERLANDS","1387104","552000","300000","191100"],["2015W08","POLAND","1387104","552000","300000","191100"],["2015W08","SLOVENIA","1387104","552000","300000","191100"],["2015W08","SWITZERLAND","1387104","552000","300000","191100"],["2015W08","UNITED KINGDOM","1387104","552000","300000","191100"],["2015W09","AUSTRIA","1504320","547200","319200","186300"],["2015W09","BELGIUM","1515309","546750","321000","185850"],["2015W09","CZECH REPUBLIC","1449375","549450","310200","188550"],["2015W09","GERMANY","1416408","550800","304800","189900"],["2015W09","IRELAND","1493331","547650","317400","186750"],["2015W09","ITALY","1482342","548100","315600","187200"],["2015W09","NETHERLANDS","1405419","551250","303000","190350"],["2015W09","POLAND","1460364","549000","312000","188100"],["2015W09","SLOVENIA","1471353","548550","313800","187650"],["2015W09","SWITZERLAND","1526298","546300","322800","185400"],["2015W09","UNITED KINGDOM","1394430","551700","301200","190800"],["2015W10","AUSTRIA","1529961","546150","323400","185250"],["2015W10","BELGIUM","1529961","546150","323400","185250"],["2015W10","CZECH REPUBLIC","1529961","546150","323400","185250"],["2015W10","GERMANY","1529961","546150","323400","185250"],["2015W10","IRELAND","1529961","546150","323400","185250"],["2015W10","ITALY","1529961","546150","323400","185250"],["2015W10","NETHERLANDS","1529961","546150","323400","185250"],["2015W10","POLAND","1529961","546150","323400","185250"],["2015W10","SLOVENIA","1529961","546150","323400","185250"],["2015W10","SWITZERLAND","1529961","546150","323400","185250"],["2015W10","UNITED KINGDOM","1529961","546150","323400","185250"],["2015W11","AUSTRIA","1529961","546150","323400","185250"],["2015W11","BELGIUM","1529961","546150","323400","185250"],["2015W11","CZECH REPUBLIC","1529961","546150","323400","185250"],["2015W11","GERMANY","1529961","546150","323400","185250"],["2015W11","IRELAND","1529961","546150","323400","185250"],["2015W11","ITALY","1529961","546150","323400","185250"],["2015W11","NETHERLANDS","1529961","546150","323400","185250"],["2015W11","POLAND","1529961","546150","323400","185250"],["2015W11","SLOVENIA","1529961","546150","323400","185250"],["2015W11","SWITZERLAND","1529961","546150","323400","185250"],["2015W11","UNITED KINGDOM","1529961","546150","323400","185250"],["2015W12","AUSTRIA","1518249","594150","323400","204450"],["2015W12","BELGIUM","1517151","598650","323400","206250"],["2015W12","CZECH REPUBLIC","1523739","571650","323400","195450"],["2015W12","GERMANY","1527033","558150","323400","190050"],["2015W12","IRELAND","1519347","589650","323400","202650"],["2015W12","ITALY","1520445","585150","323400","200850"],["2015W12","NETHERLANDS","1528131","553650","323400","188250"],["2015W12","POLAND","1522641","576150","323400","197250"],["2015W12","SLOVENIA","1521543","580650","323400","199050"],["2015W12","SWITZERLAND","1516053","603150","323400","208050"],["2015W12","UNITED KINGDOM","1529229","549150","323400","186450"],["2015W13","AUSTRIA","1632903","599850","342600","203850"],["2015W13","BELGIUM","1643892","599400","344400","203400"],["2015W13","CZECH REPUBLIC","1577958","602100","333600","206100"],["2015W13","GERMANY","1544991","603450","328200","207450"],["2015W13","IRELAND","1621914","600300","340800","204300"],["2015W13","ITALY","1610925","600750","339000","204750"],["2015W13","NETHERLANDS","1534002","603900","326400","207900"],["2015W13","POLAND","1588947","601650","335400","205650"],["2015W13","SLOVENIA","1599936","601200","337200","205200"],["2015W13","SWITZERLAND","1654881","598950","346200","202950"],["2015W13","UNITED KINGDOM","1523013","604350","324600","208350"],["2015W14","AUSTRIA","1646832","646800","346800","222000"],["2015W14","BELGIUM","1645734","651300","346800","223800"],["2015W14","CZECH REPUBLIC","1652322","624300","346800","213000"],["2015W14","GERMANY","1655616","610800","346800","207600"],["2015W14","IRELAND","1647930","642300","346800","220200"],["2015W14","ITALY","1649028","637800","346800","218400"],["2015W14","NETHERLANDS","1656714","606300","346800","205800"],["2015W14","POLAND","1651224","628800","346800","214800"],["2015W14","SLOVENIA","1650126","633300","346800","216600"],["2015W14","SWITZERLAND","1644636","655800","346800","225600"],["2015W14","UNITED KINGDOM","1657812","601800","346800","204000"],["2015W15","AUSTRIA","1644270","657300","346800","226200"],["2015W15","BELGIUM","1644270","657300","346800","226200"],["2015W15","CZECH REPUBLIC","1644270","657300","346800","226200"],["2015W15","GERMANY","1644270","657300","346800","226200"],["2015W15","IRELAND","1644270","657300","346800","226200"],["2015W15","ITALY","1644270","657300","346800","226200"],["2015W15","NETHERLANDS","1644270","657300","346800","226200"],["2015W15","POLAND","1644270","657300","346800","226200"],["2015W15","SLOVENIA","1644270","657300","346800","226200"],["2015W15","SWITZERLAND","1644270","657300","346800","226200"],["2015W15","UNITED KINGDOM","1644270","657300","346800","226200"],["2015W16","AUSTRIA","1632558","705300","346800","245400"],["2015W16","BELGIUM","1631460","709800","346800","247200"],["2015W16","CZECH REPUBLIC","1638048","682800","346800","236400"],["2015W16","GERMANY","1641342","669300","346800","231000"],["2015W16","IRELAND","1633656","700800","346800","243600"],["2015W16","ITALY","1634754","696300","346800","241800"],["2015W16","NETHERLANDS","1642440","664800","346800","229200"],["2015W16","POLAND","1636950","687300","346800","238200"],["2015W16","SLOVENIA","1635852","691800","346800","240000"],["2015W16","SWITZERLAND","1630362","714300","346800","249000"],["2015W16","UNITED KINGDOM","1643538","660300","346800","227400"],["2015W17","AUSTRIA","1629996","715800","346800","249600"],["2015W17","BELGIUM","1629996","715800","346800","249600"],["2015W17","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W17","GERMANY","1629996","715800","346800","249600"],["2015W17","IRELAND","1629996","715800","346800","249600"],["2015W17","ITALY","1629996","715800","346800","249600"],["2015W17","NETHERLANDS","1629996","715800","346800","249600"],["2015W17","POLAND","1629996","715800","346800","249600"],["2015W17","SLOVENIA","1629996","715800","346800","249600"],["2015W17","SWITZERLAND","1629996","715800","346800","249600"],["2015W17","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W18","AUSTRIA","1629996","715800","346800","249600"],["2015W18","BELGIUM","1629996","715800","346800","249600"],["2015W18","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W18","GERMANY","1629996","715800","346800","249600"],["2015W18","IRELAND","1629996","715800","346800","249600"],["2015W18","ITALY","1629996","715800","346800","249600"],["2015W18","NETHERLANDS","1629996","715800","346800","249600"],["2015W18","POLAND","1629996","715800","346800","249600"],["2015W18","SLOVENIA","1629996","715800","346800","249600"],["2015W18","SWITZERLAND","1629996","715800","346800","249600"],["2015W18","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W19","AUSTRIA","1629996","715800","346800","249600"],["2015W19","BELGIUM","1629996","715800","346800","249600"],["2015W19","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W19","GERMANY","1629996","715800","346800","249600"],["2015W19","IRELAND","1629996","715800","346800","249600"],["2015W19","ITALY","1629996","715800","346800","249600"],["2015W19","NETHERLANDS","1629996","715800","346800","249600"],["2015W19","POLAND","1629996","715800","346800","249600"],["2015W19","SLOVENIA","1629996","715800","346800","249600"],["2015W19","SWITZERLAND","1629996","715800","346800","249600"],["2015W19","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W20","AUSTRIA","1629996","715800","346800","249600"],["2015W20","BELGIUM","1629996","715800","346800","249600"],["2015W20","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W20","GERMANY","1629996","715800","346800","249600"],["2015W20","IRELAND","1629996","715800","346800","249600"],["2015W20","ITALY","1629996","715800","346800","249600"],["2015W20","NETHERLANDS","1629996","715800","346800","249600"],["2015W20","POLAND","1629996","715800","346800","249600"],["2015W20","SLOVENIA","1629996","715800","346800","249600"],["2015W20","SWITZERLAND","1629996","715800","346800","249600"],["2015W20","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W21","AUSTRIA","1629996","715800","346800","249600"],["2015W21","BELGIUM","1629996","715800","346800","249600"],["2015W21","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W21","GERMANY","1629996","715800","346800","249600"],["2015W21","IRELAND","1629996","715800","346800","249600"],["2015W21","ITALY","1629996","715800","346800","249600"],["2015W21","NETHERLANDS","1629996","715800","346800","249600"],["2015W21","POLAND","1629996","715800","346800","249600"],["2015W21","SLOVENIA","1629996","715800","346800","249600"],["2015W21","SWITZERLAND","1629996","715800","346800","249600"],["2015W21","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W22","AUSTRIA","1629996","715800","346800","249600"],["2015W22","BELGIUM","1629996","715800","346800","249600"],["2015W22","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W22","GERMANY","1629996","715800","346800","249600"],["2015W22","IRELAND","1629996","715800","346800","249600"],["2015W22","ITALY","1629996","715800","346800","249600"],["2015W22","NETHERLANDS","1629996","715800","346800","249600"],["2015W22","POLAND","1629996","715800","346800","249600"],["2015W22","SLOVENIA","1629996","715800","346800","249600"],["2015W22","SWITZERLAND","1629996","715800","346800","249600"],["2015W22","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W23","AUSTRIA","1629996","715800","346800","249600"],["2015W23","BELGIUM","1629996","715800","346800","249600"],["2015W23","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W23","GERMANY","1629996","715800","346800","249600"],["2015W23","IRELAND","1629996","715800","346800","249600"],["2015W23","ITALY","1629996","715800","346800","249600"],["2015W23","NETHERLANDS","1629996","715800","346800","249600"],["2015W23","POLAND","1629996","715800","346800","249600"],["2015W23","SLOVENIA","1629996","715800","346800","249600"],["2015W23","SWITZERLAND","1629996","715800","346800","249600"],["2015W23","UNITED KINGDOM","1629996","715800","346800","249600"],["2015W24","AUSTRIA","1629996","715800","346800","249600"],["2015W24","BELGIUM","1629996","715800","346800","249600"],["2015W24","CZECH REPUBLIC","1629996","715800","346800","249600"],["2015W24","GERMANY","1629996","715800","346800","249600"],["2015W24","IRELAND","1629996","715800","346800","249600"],["2015W24","ITALY","1629996","715800","346800","249600"],["2015W24","NETHERLANDS","1629996","715800","346800","249600"],["2015W24","POLAND","1629996","715800","346800","249600"],["2015W24","SLOVENIA","1629996","715800","346800","249600"],["2015W24","SWITZERLAND","1629996","715800","346800","249600"],["2015W24","UNITED KINGDOM","1629996","715800","346800","249600"]]};



app.config(function($routeProvider) 
{
$routeProvider
// route for the index page
.when('/', 
{
templateUrl : 'templates/landing-page.html',
controller  : 'landingPageController'
})
.when('/kpi', 
{
templateUrl : 'templates/kpi.html',
controller  : 'landingPageController'
})


.otherwise(
{
redirectTo: '/'
});


});

app.service('countryService', function() {
  var current;

  return {
    // set selected country
    set: function(country) {
      current = country;
     // console.log('set', current);
    },
    // get selected country
    get: function() {
     // console.log('get', current);
      return current;
    }
  };
}); 

app.service('visitorService', function() {
  var current;
  return {
    set: function(visitor){
      current = visitor;
      //console.log('set', current);
      
    },
    get: function(){
     // console.log('get', current);
      return current;
    }
  }
});

app.filter('commaS', function(){
	return function (input) {
      if (input) {
       return input.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      };    
	};
});

app.service('periodService', function() {
  var current = "Prior Week";
  return {
    set: function(content){
      current = content;
      //console.log('set', current);
      
    },
    get: function(){
     // console.log('get', current);
      return current;
    }
  }
});

app.filter('currencyWithNumberFilter', ['$filter','$sce', 
    function ($filter, $sce) {
        return function (input, curr) {
            var formattedValue = $filter('number')(input, 0);          
            return $sce.trustAsHtml(curr + formattedValue);
        }
    }]
  );




