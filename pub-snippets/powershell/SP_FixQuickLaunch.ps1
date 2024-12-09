Add-PSSnapin microsoft.sharepoint.powershell

function FixNavigation {
param ( $url, $Hideitems )         
    $site = New-Object  Microsoft.SharePoint.SPSite($url)    
    $web = $site.OpenWeb()         
    $httpRequest = New-Object  System.Web.HttpRequest("", $web.Url, "")    
    $stringWriter = New-Object  System.IO.StringWriter    
    $httpResponse = New-Object  System.Web.HttpResponse($stringWriter)     
    [System.Web.HttpContext]::Current = New-Object  System.Web.HttpContext($httpRequest, $httpResponse)    
    [Microsoft.SharePoint.WebControls.SPControl]::SetContextWeb([System.Web.HttpContext]::Current, $web)         
    $pubweb = [Microsoft.SharePoint.Publishing.PublishingWeb]::GetPublishingWeb($web)    
    $dictionary = New-Object  "System.Collections.Generic.Dictionary``2[[System.Int32, mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089],[Microsoft.SharePoint.Navigation.SPNavigationNode, Microsoft.SharePoint, Version=15.0.0.0, Culture=neutral, PublicKeyToken=71e9bce111e9429c]]"    
    $currentNavSettings = New-Object  System.Configuration.ProviderSettings("CurrentNavSiteMapProvider", "Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapProvider, Microsoft.SharePoint.Publishing, Version=15.0.0.0, Culture=neutral, PublicKeyToken=71e9bce111e9429c")    
    $currentNavSettings.Parameters["NavigationType"] = "Current"    
    $currentNavSettings.Parameters["EncodeOutput"] = "true"   
    [Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapProvider]$currentNavSiteMapProvider = [System.Web.Configuration.ProvidersHelper]::InstantiateProvider($currentNavSettings, [type]"Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapProvider")    
    [Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapNode]$currentNode = $currentNavSiteMapProvider.CurrentNode
    $children = $currentNode.GetNavigationChildren([Microsoft.SharePoint.Publishing.NodeTypes]::All, [Microsoft.SharePoint.Publishing.NodeTypes]::All, [Microsoft.SharePoint.Publishing.OrderingMethod]::Manual, [Microsoft.SharePoint.Publishing.AutomaticSortingMethod]::Title, $true,  -1);         
    $menuNodes = New-Object  System.Collections.ObjectModel.Collection[Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapNode]    
    foreach ($node in $children)
    {        
        $menuNodes.Add($node)    
    }         

    foreach ($menuItem in $Hideitems) 
    {        
        foreach ($menuNode in $menuNodes) 
        {         
            if ($menuNode.InternalTitle  -eq $menuItem) 
            {          
                Write-Host "Hiding: $($menuNode.InternalTitle)..." -NoNewline        
                $quickId = GetFullID $menuNode        
                if ($quickId -ne $null) 

                {            
			    $id = $quickId.Split(',');           
			    $objId = New-Object  Guid($id[0]);   
			    $pubweb.Navigation.ExcludeFromNavigation($false, $objId)
			    $pubweb.Web.Update()     
			    $pubweb.Update()
			    Write-Host "DONE"      
			    break                    
                }                          
            }        

        }                 

    }
         [System.Web.HttpContext]::Current = $null
  } 

 

function GetFullID {    

param (        [Microsoft.SharePoint.Publishing.Navigation.PortalSiteMapNode] $menuNode    )

    $fullId = $null         
    $portalSiteMapNodeType = $menuNode.GetType()    
    $property = $portalSiteMapNodeType.GetProperty("QuickId", [System.Reflection.BindingFlags] "Instance, NonPublic")    
    $fullId = [string] $property.GetValue($menuNode, $null)         
    $fullId
} 

 
Write-Host "Hiding navigation items..."
$hideitems = "XXXX","XXXXXXXX"
write-host $hideitems
FixNavigation "http://XXXXX.XXX.XXX.qld.XXX.XXX" $hideitems
Write-Host "Hiding completed." 
