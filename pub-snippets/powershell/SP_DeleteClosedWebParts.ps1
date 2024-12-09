[System.Reflection.Assembly]::Load("Microsoft.SharePoint, Version=12.0.0.0, Culture=neutral, PublicKeyToken=71e9bce111e9429c") | out-null  
[void][System.reflection.Assembly]::LoadWithPartialName("Microsoft.SharePoint") 


#Write Header to CSV File  
"Page URL,  Web Part Name" | out-file ClosedWebParts.csv 

function FixWeb([Microsoft.SharePoint.SPWeb] $web)
{  
try{

#write-host "Cleaning up web: " $web
#Get All Pages from site's Root into $AllPages Array 
$AllPages = @($web.Files | Where-Object {$_.Name -match ".aspx"}) 
  
#Search All Folders for Pages 
foreach ($folder in $web.Folders) 
    { 
       #Add the pages to $AllPages Array 
       $AllPages += @($folder.Files | Where-Object {$_.Name -match ".aspx"}) 
    } 
 #write-host $AllPages 
 #Iterate through all pages 
 foreach($Page in $AllPages) 
  { 
     $webPartManager = $web.GetLimitedWebPartManager($Page.ServerRelativeUrl, [System.Web.UI.WebControls.WebParts.PersonalizationScope]::Shared) 
     $file = $web.GetFile($Page)
     write-host "File: " $file
     #write-host "RequiresCheckOut: " $file.RequiresCheckOut
     # Array to Hold Closed Web Parts  
    $closedWebParts = @() 
                foreach ($webPart in $webPartManager.WebParts | Where-Object {$_.IsClosed -or $_.IsIncluded -eq $false}) 
                { 
                 $result = "$($web.site.Url)$($Page.ServerRelativeUrl), $($webpart.Title)"
                 #Write-Host "Closed Web Part(s) Found at: "$result
                 $result | Out-File ClosedWebParts.csv -Append 
                 $closedWebParts += $webPart
                } 
      
    #Delete Closed Web Parts 
    foreach ($webPart in $closedWebParts) 
                { 
		     write-host "File to checkout: " $file
                     try{$file.UndoCheckOut()}catch{}
                     try{$file.CheckOut()}catch{}
	             $webPartManager.Dispose()
		     $webPartManager = $web.GetLimitedWebPartManager($Page.ServerRelativeUrl, [System.Web.UI.WebControls.WebParts.PersonalizationScope]::Shared) 
      		     $webPartToDelete = ($webPartManager.WebParts | ? {$_.ID -eq $webPart.ID})
	             Write-Host "Deleting WebPart with ID: "$webPart.ID
	             try{$webPartManager.DeleteWebPart($webPartToDelete)}catch{}
                 try{$file.CheckIn("Deleted web part: " + $webpart.Title)}catch{}
                 }

	      $webPartManager.Dispose()
              $file.Dispose()
	      $Page.Dispose()
     
 } 
}catch{}
finally{}
}

function Pause ($Message="Press any key to continue...")
{
 Write-Host -NoNewLine $Message
 #$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
 Write-Host ""
}

write-host "This script will delete all web parts on this server that are"
write-host "currently in a closed (not displayed) status."
pause

$oContentService = [Microsoft.SharePoint.Administration.SPWebService]::ContentService;
[Microsoft.SharePoint.Administration.SPWebApplicationCollection]$waColl = $oContentService.WebApplications;

$waColl1 = $waColl | where-object {$_.IsAdministrationWebApplication -eq $FALSE} 

foreach ($wa in $waColl1) 
{ 
  write-host $wa.Name
  $waName = $wa.Name
  $sites = $wa.Sites 

  foreach ($obj in $sites)
  {
    try{
    $spSite = new-object Microsoft.SharePoint.SPSite($obj.Url)
    $webs=$spSite.AllWebs

    If ($webs -ne $null)
    {
      foreach ($web in $webs) 
      {
        FixWeb $web
        $web.Dispose()
      }
    }
    }catch{}
    
    $webs = $null

  }
}


