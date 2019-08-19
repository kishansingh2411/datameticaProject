(function($)
	{

	var recognition;
	var recognizing = false;
	$.init = function($location)
	{
		if (!('webkitSpeechRecognition' in window)) 
		{
  			
		}
		else 
		{
  		 recognition = new webkitSpeechRecognition();
  		 recognition.continuous = true;
  		 recognition.interimResults = true;
  		 recognition.onstart = function() { 
  		 	 recognizing = true;

  		 }


  		   recognition.onresult = function(event) 
  		   {
    			var saidWord = '';
    		  for (var i = event.resultIndex; i < event.results.length; ++i) 
    		{
      			if (event.results[i].isFinal) 
      			{
        		final_transcript = event.results[i][0].transcript;
      			} else 
      			{
       			 saidWord = event.results[i][0].transcript;
      			}
    		}
		    //console.log(final_transcript);
		    console.log(saidWord + " "+saidWord.length);
    		if(saidWord=="average revenue per order"||saidWord=="average revenue"||saidWord=="per order")
    		{
    			
    			window.location.replace("http://localhost/office/depot/home.html#/avgRevenue");
    			console.log("Opening ARPO");
    		}
    		else if(saidWord=="ocr")
    		{
    			
    			window.location.replace("http://localhost/office/depot/home.html#/charts");
    			console.log("Opening OCR");
    		}	
        else if(saidWord=="average page"||saidWord=="average page view")
        {
          
          window.location.replace("http://localhost/office/depot/home.html#/avgPageViews");
          console.log("Opening OCR");
        } 
        else if(saidWord=="new visitors"||saidWord=="percent revenue")
        {
          
          window.location.replace("http://localhost/office/depot/home.html#/avgRevNewVistor");
          console.log("Opening OCR");
        } 

         else if(saidWord=="exist visitors"||saidWord=="percent revenue")
        {
          
          window.location.replace("http://localhost/office/depot/home.html#/avgRevExistVisitors");
          console.log("Opening OCR");
        } 
        else if(saidWord=="shopping cart"||saidWord=="completion")
        {
          window.location.replace("http://localhost/office/depot/home.html#/shoppingCartCompletion");
          console.log("Opening OCR");
        } 




  			};

  		 recognition.onerror = function(event) 
  		 {
  		 	if (event.error == 'no-speech') 
  		 	{

      		ignore_onend = true;
    		}
		    if (event.error == 'audio-capture') 
		    {
		  
		      ignore_onend = true;
		    }
		    if (event.error == 'not-allowed') 
		    {
		      if (event.timeStamp - start_timestamp < 100) 
		      {
		       

		      } else 
		      {
		       
		      }
		      ignore_onend = true;
		    }


  		};
  		recognition.onend = function() 
  		{  
  		 	recognizing = false;
  		}
  		 
  		this.start();

}
	}; /*Speech Init Start*/
	$.start =  function(event) 
  		 {
 
 			 final_transcript = '';
  			 recognition.lang = "en-IN" /*en-IN  en-US*/
  			 recognition.start();
		 };

		
	})(jQuery);