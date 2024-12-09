#Please note you may need to execute: C:\Program Files\D365SDK\SDK\Bin\RegisterXRMTooling.ps1

Add-PSSnapin Microsoft.Xrm.Tooling.Connector
#Import the required DLL
Add-Type -Path  'C:\Program Files\Common Files\Microsoft Shared\Web Server Extensions\15\ISAPI\Microsoft.SharePoint.Client.dll'

function Action-Refresh-ClearAndDeployAll
{
    Action-Refresh-Accounts
    Action-Refresh-Contacts
    Action-Refresh-Opportunities
}

function Action-Refresh-Accounts #Must be First
{
    Action-Export-Accounts
    Action-Delete-All-Accounts
    Action-CreateFromCSV-Accounts    
}

function Action-Refresh-Contacts #Must be Second
{
    Action-Export-Contacts
    Action-Delete-All-Contacts
    Action-CreateFromCSV-Contacts
}

function Action-Refresh-Opportunities #Must be Third
{
    Action-Export-Opportunities
    Action-Delete-All-Opportunities
    Action-Delete-All-OpportunityClose    
    Action-Delete-All-Opportunities
    Action-Delete-All-OpportunityClose    
    Action-CreateFromCSV-Opportunities    
}

function Action-Export-Accounts
{
    #set execution policy  =remote signed
    Update-FormatData -AppendPath "C:\XXXXXXXX\SPClient.Format\SPClient.Format.ps1xml"
    $GLOBAL:Context = New-Object Microsoft.SharePoint.Client.ClientContext("https://XXXX.XXXXX.XXXXX/sales")
    $username = "XXXXXX.XXXXX@XXXXX.XXXXX"
    $password = ConvertTo-SecureString "XXXXX" -AsPlainText -Force
    $Context.Credentials = New-Object System.Net.NetworkCredential($username, $password)
    $GLOBAL:Web = $GLOBAL:Context.Web
    $GLOBAL:Context.Load($GLOBAL:Web)
    $GLOBAL:Context.ExecuteQuery()
    $SPList = $GLOBAL:Web.Lists.GetByTitle("Client Master List")
    write-host $GLOBAL:Web.Title
    $listItems = $SPList.GetItems([Microsoft.SharePoint.Client.CamlQuery]::CreateAllItemsQuery())  
    $GLOBAL:Context.Load($listItems)  
    $GLOBAL:Context.ExecuteQuery()  
    $outputArr = @()
    foreach($listItem in $listItems)  
    {  
        $outputArr += $listItem | select -Property @{n='ClientCode';e={$_["Client_x0020_Code"]}},@{n='Client';e={$_["Title"]}},@{n='Suburb';e={$_["w1z6"]}},@{n='Region';e={$_["u47s"]}},@{n='Country';e={$_["soas"]}},@{n='PostCode';e={$_["inhn"]}},@{n='Address';e={$_["cepa"]}},@{n='Phone';e={$_["Phone"]}},@{n='PrimaryContactName';e={$_["Contact_x0020_Name"]}},@{n='Notes';e={$_["Notes"]}},@{n='AccountManager';e={try {$_["Account_x0020_Picker"].LookupValue}catch{""}}}
    }  
    $outputArr | export-csv 'accounts.csv'
}


function Action-Export-Contacts
{
    #set execution policy  =remote signed
    Update-FormatData -AppendPath "C:\XXXX\XXXX\XXXX\X\SPClient.Format\SPClient.Format.ps1xml"
    $GLOBAL:Context = New-Object Microsoft.SharePoint.Client.ClientContext("https://XXXX.XXX.XXX/sales")
    $username = "XXXXX.XXXX@XXXX.XXXX"
    $password = ConvertTo-SecureString "XXXXX" -AsPlainText -Force
    $Context.Credentials = New-Object System.Net.NetworkCredential($username, $password)
    $GLOBAL:Web = $GLOBAL:Context.Web
    $GLOBAL:Context.Load($GLOBAL:Web)
    $GLOBAL:Context.ExecuteQuery()
    $SPList = $GLOBAL:Web.Lists.GetByTitle("Customer Contacts") #Note that BigListLotsOfItems is my List Title
    write-host $GLOBAL:Web.Title
    $listItems = $SPList.GetItems([Microsoft.SharePoint.Client.CamlQuery]::CreateAllItemsQuery())  
    $GLOBAL:Context.Load($listItems)  
    $GLOBAL:Context.ExecuteQuery()  
    $outputArr = @()
    foreach($listItem in $listItems)  
    {  
        $outputArr += $listItem | select -Property @{n='FullName';e={$_["Title"]}},@{n='EmailAddress';e={$_["Email"]}},@{n='Company';e={$_["Company"]}},@{n='JobTitle';e={$_["JobTitle"]}},@{n='BusinessPhone';e={$_["WorkPhone"]}},@{n='HomePhone';e={$_["HomePhone"]}},@{n='MobileNumber';e={$_["CellPhone"]}},@{n='FaxNumber';e={$_["WorkFax"]}},@{n='Address';e={$_["WorkAddress"]}},@{n='Suburb';e={$_["WorkCity"]}},@{n='State';e={$_["WorkState"]}},@{n='PostCode';e={$_["WorkZip"]}},@{n='Country';e={$_["WorkCountry"]}},@{n='WebPage';e={$_["WebPage"].Url}},@{n='Description';e={$_["Comments"]}},@{n="Customer";e={try {$_["Customer"].LookupValue}catch{""}}}
    }  
    $outputArr | export-csv 'contacts.csv'
}

function Action-Export-Opportunities
{
    #set execution policy  =remote signed
    Update-FormatData -AppendPath "C:\XXXXXX\SPClient.Format\SPClient.Format.ps1xml"
    $GLOBAL:Context = New-Object Microsoft.SharePoint.Client.ClientContext("https://XXXX.XXXXX.XXXXX/sales")
    $username = "XXXXX.XXXX@XXXXX.XXXX"
    $password = ConvertTo-SecureString "XXXXX" -AsPlainText -Force
    $Context.Credentials = New-Object System.Net.NetworkCredential($username, $password)
    $GLOBAL:Web = $GLOBAL:Context.Web
    $GLOBAL:Context.Load($GLOBAL:Web)
    $GLOBAL:Context.ExecuteQuery()
    $SPList = $GLOBAL:Web.Lists.GetByTitle("Sales Opportunities") #Note that BigListLotsOfItems is my List Title
    write-host $GLOBAL:Web.Title
    $listItems = $SPList.GetItems([Microsoft.SharePoint.Client.CamlQuery]::CreateAllItemsQuery())  
    $GLOBAL:Context.Load($listItems)  
    $GLOBAL:Context.ExecuteQuery()  
    $outputArr = @()
    foreach($listItem in $listItems)  
    {  
        $outputArr += $listItem | select -Property @{n='ProjectName';e={$_["Title"]}},@{n='Discoveredby';e={$_["Discovered_x0020_by"].LookupValue}},@{n='Owner';e={$_["Owner"].LookupValue}},@{n='Stream';e={$_["Stream"].LookupValue}},@{n='Client';e={$_["Client"].LookupValue}},@{n='EstimatedValue';e={$_["Estimated_x0020_Value"]}},@{n='EstimatedEngineeringHours';e={$_["Estimated_x0020_Engineering_x002"]}},@{n='ExpectedProjectDuration';e={$_["Expected_x0020_Project_x0020_Dur"]}},@{n='Region';e={$_["Region"].LookupValue}},@{n='Zone';e={$_["Zone"].LookupValue}},@{n='VerticalIndustry';e={$_["Vertical_x0020_Industry"].LookupValue}},@{n='ProposedAwardDate';e={$_["Proposed_x0020_Award_x0020_Date"]}},@{n='FirstRevenueDate';e={$_["First_x0020_Revenue_x0020_Date"]}},@{n='WonLostDate';e={$_["WonLost_x0020_Date"]}},@{n='ExistingCustomer';e={$_["Existing_x0020_Customer"]}},@{n="ProjectDescription";e={$_["Project_x0020_Description"]}},@{n="WinProbability";e={$_["Win_x0020_Probability_x0020__x00"]}},@{n="ExpectedMargin";e={$_["Expected_x0020_Margin_x0020__x00"]}},@{n="SalesPhase";e={$_["Sales_x0020_Phase"].LookupValue}}
    }  
    $outputArr | export-csv 'opportunities.csv'
}


function Get-Crm-Connectioncrm_
{
    # Load SDK assemblie
    Add-Type -Path "C:\Program Files\D365SDK\SDK\Bin\Microsoft.Xrm.Sdk.dll";
    Add-Type -Path "C:\Program Files\D365SDK\SDK\Bin\Microsoft.Xrm.Tooling.Connector.dll";
    Add-Type -Path "C:\Program Files\D365SDK\SDK\Bin\Microsoft.Crm.Sdk.Proxy.dll";

    # Configure CRM connection
    $secpasswd = ConvertTo-SecureString "@9z9z9z9z" -AsPlainText -Force
    $cred = New-Object System.Management.Automation.PSCredential ("email@domain.com", $secpasswd)   
    return Get-CrmConnection –ServerUrl "https://xxxxx.crm6.dynamics.com" -Credential $cred -OrganizationName "XXXXX CRM PROD" 
}
function Get-Multiple-Records
{
    PARAM
    (
        [parameter(Mandatory=$true)]$service,
        [parameter(Mandatory=$true)]$query
    )

    $pageNumber = 1;

    $query.PageInfo = New-Object -TypeName Microsoft.Xrm.Sdk.Query.PagingInfo;
    $query.PageInfo.PageNumber = $pageNumber;
    $query.PageInfo.Count = 1000;
    $query.PageInfo.PagingCookie = $null;

    $records = $null;
    while($true)
    {
        $results = $service.RetrieveMultiple($query);
                
        Write-Progress -Activity "Retrieve data from CRM" -Status "Processing record page : $pageNumber" -PercentComplete -1;
        if($results.Entities.Count -gt 0)
        {
            if($records -eq $null)
            {
                $records = $results.Entities;
            }
            else
            {
                $records.AddRange($results.Entities);
            }
        }
        if($results.MoreRecords)
        {
            $pageNumber++;
            $query.PageInfo.PageNumber = $pageNumber;
            $query.PageInfo.PagingCookie = $results.PagingCookie;
        }
        else
        {
            break;
        }
    }
    return $records;
}


function Action-Get-Users
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "systemuser";
    $query.ColumnSet.AddColumn("internalemailaddress");
    $query.ColumnSet.AddColumn("fullname");
    $query.ColumnSet.AddColumn("firstname");
    $query.ColumnSet.AddColumn("lastname");
    #qe.LinkEntities.Add(new LinkEntity("account", "contact", "primarycontactid", "contactid", JoinOperator.Inner));
    #qe.LinkEntities[0].Columns.AddColumns("firstname", "lastname");
    #qe.LinkEntities[0].EntityAlias = "primarycontact";
    return Get-Multiple-Records $service $query;
}



function Action-Get-Accounts
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "account";
    $query.ColumnSet.AddColumn("name");
    return Get-Multiple-Records $service $query;
}


function Action-Get-Opportunities
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "opportunity";
    $query.ColumnSet.AddColumn("name");
    $query.ColumnSet.AddColumn("traversedpath");
    $query.ColumnSet.AddColumn("processid");
    $query.ColumnSet.AddColumn("stageid");
    return Get-Multiple-Records $service $query;
}


function Action-Get-Leads
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "lead";
    return Get-Multiple-Records $service $query;
}
function Action-Get-OpportunityClose
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "opportunityclose";
    $query.ColumnSet.AddColumn("activityid");
    $query.ColumnSet.AddColumn("opportunityid");
    $query.ColumnSet.AddColumn("statecode");
    $query.ColumnSet.AddColumn("statuscode");
    return Get-Multiple-Records $service $query;
}


function Action-Get-Contacts
{
    $service = Get-Crm-Connection
    $query = New-Object -TypeName Microsoft.Xrm.Sdk.Query.QueryExpression -ArgumentList "contact";
    $query.ColumnSet.AddColumn("firstname");
    $query.ColumnSet.AddColumn("lastname");
    return Get-Multiple-Records $service $query;
}

function Action-Delete-All-Accounts
{
    $service = Get-Crm-Connection
    $allaccounts = Action-Get-Accounts
    foreach ($account in $allaccounts)
    { 
        Write-Progress -Activity "Deleting from CRM" -Status "Processing record with ID: $account.Id" -PercentComplete -1;
      
        $service.Delete("account",$account.Id)
    }
}

function Action-Delete-All-Leads
{
    $service = Get-Crm-Connection
    $allleads = Action-Get-Leads
    foreach ($lead in $allleads)
    { 
        Write-Progress -Activity "Deleting from CRM" -Status "Processing record with ID: $lead.Id" -PercentComplete -1;     
        $service.Delete("lead",$lead.Id)
    }
}


function Action-Delete-All-Opportunities
{
    $service = Get-Crm-Connection
    $allopportunities = Action-Get-Opportunities
    foreach ($opportunity in $allopportunities)
    { 
        Write-Progress -Activity "Deleting from CRM" -Status "Processing record with ID: $opportunity.Id" -PercentComplete -1;
        
        $service.Delete("opportunity",$opportunity.Id)
        Write-host "deleted opp"
    }
}


function Action-Delete-All-Contacts
{
    $service = Get-Crm-Connection
    $allcontacts = Action-Get-Contacts
    foreach ($contact in $allcontacts)
    { 
        Write-Progress -Activity "Deleting from CRM" -Status "Processing record with ID: $contact.Id" -PercentComplete -1;
        $service.Delete("contact",$contact.Id)
    }
}

function Action-Delete-All-OpportunityClose
{
    $service = Get-Crm-Connection
    $allclose = Action-Get-OpportunityClose
    foreach ($close in $allclose)
    { 
        Write-Progress -Activity "Deleting from CRM" -Status "Processing record with ID: $close.Id" -PercentComplete -1;
        $service.Delete("contact",$close.Id)
    }
}




function Action-CreateFromCSV-Accounts
{
    $importCSVPath= "accounts.csv"
    $service = Get-Crm-Connection
    $importTable = @()
    $importTable = import-csv $importCSVPath | select "ClientCode","Client","Suburb","Region","Country","PostCode","Address","Phone","PrimaryContactName","Notes","AccountManager"
    foreach ($row in $importTable)
    {
        $account = New-Object -TypeName Microsoft.Xrm.Sdk.Entity -ArgumentList "account"
        $account["new_clientcode"] = $row.ClientCode
        $account["name"] = $row.Client
        $account["address1_city"] = $row.Suburb
        $account["address1_stateorprovince"] = $row.Region
        $account["address1_country"] = $row.Country
        $account["address1_postalcode"] = $row.PostCode
        $account["address1_line1"] = $row.Address
        $account["telephone1"] = $row.Phone
        $account["address1_primarycontactname"] = $row.PrimaryContactName
        $account["description"] = $row.Notes

        #look up account manager from systemuser(s) if not found use David XXXX instead
        try
        {
            $users = Action-Get-Users
            $user = $users | ? {$_.Attributes["fullname"] -eq $row.AccountManager} 
            $dave = $users | ? {$_.Attributes["fullname"] -eq "David"} 
            if ($user -ne $null)
            {
               $account["ownerid"] = $user.ToEntityReference()
            }else
            {
               $account["ownerid"] = $dave.ToEntityReference()
            }
            
        }catch{}
        $service.Create($account)
        #return #one interation for testing only...
    }
        
}

function Action-CreateFromCSV-Contacts
{
    $importCSVPath= "contacts.csv"
    $service = Get-Crm-Connection
    $importTable = @()
    $importTable = import-csv $importCSVPath | select "FullName", "EmailAddress","Company","JobTitle", "BusinessPhone", "HomePhone","MobileNumber","FaxNumber" , "Address" , "Suburb","State","PostCode","Country","WebPage","Description","Customer"
    foreach ($row in $importTable)
    {
        $contact = New-Object -TypeName Microsoft.Xrm.Sdk.Entity -ArgumentList "contact"
        if ($row.FullName -ne "")
        {
            write-host $row.FullName
            $contact["firstname"] = $row.FullName.Split(" ")[0]
            $contact["lastname"] = $row.FullName.Split(" ")[1]
        }
        $contact["emailaddress1"] = $row.EmailAddress
        #$contact["company"] = $row.Company
        $contact["jobtitle"] = $row.JobTitle
        $contact["telephone1"] = $row.BusinessPhone
        $contact["telephone2"] = $row.HomePhone
        $contact["mobilephone"] = $row.MobileNumber
        $contact["address1_fax"] = $row.FaxNumber
        $contact["address1_line1"] = $row.Address
        $contact["address1_city"] = $row.Suburb
        $contact["address1_stateorprovince"] = $row.State
        $contact["address1_postalcode"] = $row.PostCode
        $contact["address1_country"] = $row.Country
        $contact["websiteurl"] = $row.WebPage

        $contact["description"] = $row.Description

        #look up account manager from systemuser(s) if not found use David XXXXXX instead
        try
        {
            $accounts = Action-Get-Accounts
            $account = $accounts  | ? {$_.Attributes["name"] -eq $row.Customer} 
            $tkg = $accounts | ? {$_.Attributes["name"] -eq "XXXXX Group"} 
            if ($account -ne $null)
            {
               $contact["parentcustomerid"] = $account.ToEntityReference()
            }else
            {
               $contact["parentcustomerid"] = $tkg.ToEntityReference()
            }
            
        }catch{}


        $service.Create($contact) #done object will be visible in CRM
        #return #one interation for testing only...
    }
        
}



function Action-CreateFromCSV-Opportunities
{

    #traversedpath 
    $importCSVPath= "opportunities.csv"
    $service = Get-Crm-Connection
    
    $importTable = @()
    $importTable = import-csv $importCSVPath | select "ProjectName", "Discoveredby","Owner","Stream", "Client", "EstimatedValue","EstimatedEngineeringHours","ExpectedProjectDuration" , "Region" , "VerticalIndustry","ProposedAwardDate","FirstRevenueDate","WonLostDate","ExistingCustomer","ProjectDescription","WinProbability","ExpectedMargin","SalesPhase"
    foreach ($row in $importTable)
    {
        $opportunity = New-Object -TypeName Microsoft.Xrm.Sdk.Entity -ArgumentList "opportunity"
        $opportunity["name"]=$row.ProjectName
        if ($row.SalesPhase -eq "Quoted")
        {
            #progress to third stage
            $opportunity["processid"]=[Guid]::Parse("3e8ebee6-a2bc-4451-9c5f-b146b085413a")
            $opportunity["stageid"]=[Guid]::Parse("d3ca8878-8d7b-47b9-852d-fcd838790cfd")
            $opportunity["traversedpath"]="6b9ce798-221a-4260-90b2-2a95ed51a5bc,650e06b4-789b-46c1-822b-0da76bedb1ed,d3ca8878-8d7b-47b9-852d-fcd838790cfd"
        }
       
        try
        {
            $users = Action-Get-Users
            $user = $users | ? {$_.Attributes["fullname"] -eq $row.Discoveredby} 
            $dave = $users | ? {$_.Attributes["fullname"] -eq "David XXXXXXX"} 
            if ($user -ne $null)
            {
               $opportunity["new_discoveredby"] = $user.ToEntityReference()
            }else
            {
               $opportunity["new_discoveredby"] = $dave.ToEntityReference()
            }
        }catch{}
        try
        {
            $users = Action-Get-Users
            $user = $users | ? {$_.Attributes["fullname"] -eq $row.Owner} 
            $dave = $users | ? {$_.Attributes["fullname"] -eq "David XXXXXXX"} 
            if ($user -ne $null)
            {
               $opportunity["ownerid"] = $user.ToEntityReference()
            }else
            {
               $opportunity["ownerid"] = $dave.ToEntityReference()
            }
        }catch{}

        $stream_options = GetOptionSet "opportunity" "new_stream"
        $opportunity["new_stream"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($stream_options[$row.Stream]))
     
        
        
        #look up account manager from systemuser(s) if not found use David XXXXXX instead
        try
        {
            $accounts = Action-Get-Accounts
            $account = $accounts  | ? {$_.Attributes["name"] -eq $row.Client} 
            $tkg = $accounts | ? {$_.Attributes["name"] -eq "XXXXXXX Group"} 
            if ($account -ne $null)
            {
               $opportunity["customerid"] = $account.ToEntityReference()
            }else
            {
               $opportunity["customerid"] = $tkg.ToEntityReference()
            }
            
        }catch{}
        $opportunity["isrevenuesystemcalculated"]=$false
        $opportunity["estimatedvalue"]=[Microsoft.Xrm.Sdk.Money](new-object Microsoft.Xrm.Sdk.Money([decimal]$row.EstimatedValue))
        $opportunity["new_estimatedengineeringhours"]=[int]$row.EstimatedEngineeringHours
        if ($row.ExpectedProjectDuration -ne "" -and $row.ExpectedProjectDuration -ne $null)
        {
        $opportunity["new_expectedprojectdurationmonths"]=[int]$row.ExpectedProjectDuration
        }
        
        if ($row.Region -ne "" -and $row.Region -ne $null)
        {
        $region_options = GetOptionSet "opportunity" "new_region"
        $opportunity["new_region"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($region_options[$row.Region]))
        }

        if ($row.Zone -ne "" -and $row.Zone -ne $null)
        {
        $zone_options = GetOptionSet "opportunity" "new_zone"
        $opportunity["new_zone"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($zone_options[$row.Zone]))
        }

        if ($row.VerticalIndustry -ne "" -and $row.VerticalIndustry -ne $null)
        {
        $industry_options = GetOptionSet "opportunity" "new_verticalindustry"
        $opportunity["new_verticalindustry"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($industry_options[$row.VerticalIndustry]))
        }

        if ($row.ProposedAwardDate -ne "" -and $row.ProposedAwardDate -ne $null)
        {
            $opportunity["finaldecisiondate"]=[datetime]::Parse($row.ProposedAwardDate)
        }
        if ($row.FirstRevenueDate -ne "" -and $row.FirstRevenueDate -ne $null)
        {
            $opportunity["new_firstrevenuedate"]=[datetime]::Parse($row.FirstRevenueDate)
        }
        if ($row.WonLostDate -ne "" -and $row.WonLostDate -ne $null)
        {
            $opportunity["actualclosedate"]=[datetime]::Parse($row.WonLostDate)
        }
        #$opportunity["new_existingcustomer"]=$row.ExistingCustomer
        $opportunity["quotecomments"]=$row.ProjectDescription
        
        if ($row.WinProbability -ne "" -and $row.WinProbability -ne $null)
        {
        $WinProbability_options = GetOptionSet "opportunity" "new_winprobability"
        $opportunity["new_winprobability"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($WinProbability_options[$row.WinProbability]))
        }
        
        if ($row.ExpectedMargin -ne "" -and $row.ExpectedMargin -ne $null)
        {
        $ExpectedMargin_options = GetOptionSet "opportunity" "new_expectedmargin"
        $opportunity["new_expectedmargin"] = [Microsoft.Xrm.Sdk.OptionSetValue](new-object Microsoft.Xrm.Sdk.OptionSetValue($ExpectedMargin_options[$row.ExpectedMargin]))
        }
        

        
        
        $opid = $service.Create($opportunity) #done object will be visible in CRM
        

        if ($row.SalesPhase -eq "Won")
        {
        sleep -Milliseconds 500 # help prevent error "already closed"
            $close= New-Object -TypeName Microsoft.Xrm.Sdk.Entity -ArgumentList "opportunityclose"
            #$opportunity["traversedpath"]="6b9ce798-221a-4260-90b2-2a95ed51a5bc,650e06b4-789b-46c1-822b-0da76bedb1ed,d3ca8878-8d7b-47b9-852d-fcd838790cfd,bb7e830a-61bd-441b-b1fd-6bb104ffa027"
            if ($row.WonLostDate -ne "" -and $row.WonLostDate -ne $null)
            {
                $close["actualend"]=[datetime]::Parse($row.WonLostDate)
            }        
            $close["actualrevenue"]=[Microsoft.Xrm.Sdk.Money](new-object Microsoft.Xrm.Sdk.Money([decimal]$row.EstimatedValue))
            $close["description"]="Imported via PowerShell"
            #            $close["opportunityid"]=$opid
            
            try
            {
                $opportunitiesCol = Action-Get-Opportunities
                $retOpp = $opportunitiesCol | ? {$_.Attributes["opportunityid"] -eq $opid} 
                if ($retOpp -ne $null)
                {
                    $close["opportunityid"] = $retOpp.ToEntityReference()
                }
            }catch{}
            
            #$closeID = $service.Create($close) #DO NOT CREATE THE OPPORTUNITYCLOSE ENTITY IT WILL BE CREATED ON THE WinOpportunityRequest
            $winoppRequest = New-Object -TypeName Microsoft.Crm.Sdk.Messages.WinOpportunityRequest
            $winoppRequest.OpportunityClose = $close
            $winoppRequest.Status = new-object Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList 3
            $service.Execute($winoppRequest)
        }
        if ($row.SalesPhase -eq "Lost" -or $row.SalesPhase -eq "Cancelled" -or $row.SalesPhase -eq "No Bid" )
        {
        sleep -Milliseconds 500 # help prevent error "already closed"
            $close= New-Object -TypeName Microsoft.Xrm.Sdk.Entity -ArgumentList "opportunityclose"
            #$opportunity["traversedpath"]="6b9ce798-221a-4260-90b2-2a95ed51a5bc,650e06b4-789b-46c1-822b-0da76bedb1ed,d3ca8878-8d7b-47b9-852d-fcd838790cfd,bb7e830a-61bd-441b-b1fd-6bb104ffa027"
            if ($row.WonLostDate -ne "" -and $row.WonLostDate -ne $null)
            {
                $close["actualend"]=[datetime]::Parse($row.WonLostDate)
            }        
            $close["actualrevenue"]=[Microsoft.Xrm.Sdk.Money](new-object Microsoft.Xrm.Sdk.Money([decimal]$row.EstimatedValue))
            $close["description"]="Imported via PowerShell"
            #$close["opportunityid"]=$opid
            try
            {
                $opportunitiesCol = Action-Get-Opportunities
                $retOpp = $opportunitiesCol | ? {$_.Attributes["opportunityid"] -eq $opid} 
                if ($retOpp -ne $null)
                {
                    $close["opportunityid"] = $retOpp.ToEntityReference()
                }
            }catch{}
            
            #$closeID = $service.Create($close) #DO NOT CREATE THE OPPORTUNITYCLOSE ENTITY IT WILL BE CREATED ON THE WinOpportunityRequest
            $winoppRequest = New-Object -TypeName Microsoft.Crm.Sdk.Messages.LoseOpportunityRequest
            $winoppRequest.OpportunityClose = $close
            $winoppRequest.Status = new-object Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList 4
            $service.Execute($winoppRequest)
        }
        

     
    #return #one interation for testing only...
 }
}


function GetOptionSet([string]$entityName, [string]$attributeName)
{
    # Request the attribute metadata
    $metadataRequest = New-Object Microsoft.Xrm.Sdk.Messages.RetrieveAttributeRequest
    $metadataRequest.EntityLogicalName = $entityName
    $metadataRequest.LogicalName = $attributeName
    $metadataRequest.RetrieveAsIfPublished = $true
    $metadataResponse = $service.Execute($metadataRequest)
    $dict = @{ };
    $metadataResponse.AttributeMetadata.OptionSet.Options | ForEach-Object {
        $label = $_.Label.UserLocalizedLabel.Label
        $value = $_.Value
        $dict.Add($label, $value)
    }
 
    $dict.Add("", $metadataResponse.AttributeMetadata.DefaultFormValue)
 
    return $dict