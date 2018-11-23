$BearerToken="dapibce755191d9eb5458a6da53ac90a55f9"
$Region="centralindia"
$JobName="TestJobRelease"
$ClusterId="0926-081131-crick762"
$SparkVersion="4.3.x-scala2.11"
$NodeType="Standard_D3_v2"
$DriverNodeType="Standard_D3_v2"
$MinNumberOfWorkers="2"
$MaxNumberOfWorkers="2"
$MaxRetries="1"
$ScheduleCronExpression="0 */4 * * * *"
$Timezone="UTC"
$NotebookPath="/Shared/GitProd/dropGIT/cdc_poc"


Function Get-DatabricksJobs
{ 
    [cmdletbinding()]
    param (
        [parameter(Mandatory = $true)][string]$BearerToken, 
        [parameter(Mandatory = $true)][string]$Region
    ) 

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $InternalBearerToken =  "Bearer $BearerToken" 
    $Region = $Region.Replace(" ","")
    
    Try {
        $Jobs = Invoke-RestMethod -Method Get -Uri "https://$Region.azuredatabricks.net/api/2.0/jobs/list" -Headers @{Authorization = $InternalBearerToken}
    }
    Catch {
        Write-Output "StatusCode:" $_.Exception.Response.StatusCode.value__ 
        Write-Output "StatusDescription:" $_.Exception.Response.StatusDescription
        Write-Error $_.ErrorDetails.Message
    }

    Return $Jobs.jobs
}

Function Add-DatabricksNotebookJob {  
    [cmdletbinding()]
    param (
        [parameter(Mandatory = $true)][string]$BearerToken,    
        [parameter(Mandatory = $true)][string]$Region,
        [parameter(Mandatory = $true)][string]$JobName,
        [parameter(Mandatory = $false)][string]$ClusterId,
        [parameter(Mandatory = $false)][string]$SparkVersion,
        [parameter(Mandatory = $false)][string]$NodeType,
        [parameter(Mandatory = $false)][string]$DriverNodeType,
        [parameter(Mandatory = $false)][int]$MinNumberOfWorkers,
        [parameter(Mandatory = $false)][int]$MaxNumberOfWorkers,
        [parameter(Mandatory = $false)][int]$Timeout,
        [parameter(Mandatory = $false)][int]$MaxRetries,
        [parameter(Mandatory = $false)][string]$ScheduleCronExpression,
        [parameter(Mandatory = $false)][string]$Timezone,
        [parameter(Mandatory = $true)][string]$NotebookPath,
        [parameter(Mandatory = $false)][string]$NotebookParametersJson,
        [parameter(Mandatory = $false)][string[]]$Libraries
    ) 

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $InternalBearerToken = "Bearer $BearerToken"
    $Region = $Region.Replace(" ","")

    $ExistingJobs = Get-DatabricksJobs -BearerToken $BearerToken -Region $Region

    $ExistingJobDetail = $ExistingJobs | Where-Object {$_.settings.name -eq $JobName} | Select-Object job_id -First 1

    if ($ExistingJobDetail){
        $JobId = $ExistingJobDetail.job_id[0]
        Write-Verbose "Updating JobId: $JobId"
        $Mode = "reset"
    } 
    else{
        $Mode = "create"
    }

    $JobBody = @{}
    $JobBody['name'] = $JobName

    If ($ClusterId){
        $JobBody['existing_cluster_id'] = $ClusterId
    }
    else {
        $ClusterDetails = @{}
        $ClusterDetails['node_type_id'] = $NodeType
        $DriverNodeType = if ($PSBoundParameters.ContainsKey('DriverNodeType')) { $DriverNodeType } else { $NodeType }
        $ClusterDetails['driver_node_type_id'] = $DriverNodeType
        $ClusterDetails['spark_version'] = $SparkVersion
        If ($MinNumberOfWorkers -eq $MaxNumberOfWorkers){
            $ClusterDetails['num_workers'] = $MinNumberOfWorkers
        }
        else {
            $ClusterDetails['autoscale'] = @{"min_workers"=$MinNumberOfWorkers;"max_workers"=$MaxNumberOfWorkers}
        }
        $JobBody['new_cluster'] = $ClusterDetails
    }

    If ($PSBoundParameters.ContainsKey('Timeout')) {$JobBody['timeout_seconds'] = $Timeout}
    If ($PSBoundParameters.ContainsKey('MaxRetries')) {$JobBody['max_retries'] = $MaxRetries}
    If ($PSBoundParameters.ContainsKey('ScheduleCronExpression')) {
        $JobBody['schedule'] = @{"quartz_cron_expression"=$ScheduleCronExpression;"timezone_id"=$Timezone}
    }
    
    $Notebook = @{}
    $Notebook['notebook_path'] = $NotebookPath
    If ($PSBoundParameters.ContainsKey('NotebookParametersJson')) {
        $Notebook['base_parameters'] = $NotebookParametersJson | ConvertFrom-Json
    }

    $JobBody['notebook_task'] = $Notebook

    If ($PSBoundParameters.ContainsKey('Libraries')) {
        If ($Libraries.Count -eq 1) {
            $Libraries += '{"DummyKey":"1"}'
        }
        $JobBody['libraries'] = $Libraries | ConvertFrom-Json
    }

    If ($Mode -eq 'create'){
        $Body = $JobBody
    }
    else {
        $Body = @{}
        $Body['job_id']= $JobId
        $Body['new_settings'] = $JobBody
    }

    $BodyText = $Body | ConvertTo-Json -Depth 10
   # $BodyText = Remove-DummyKey $BodyText

    Write-Verbose $BodyText
  
    Try {
        $JobDetails = Invoke-RestMethod -Method Post -Body $BodyText -Uri "https://$Region.azuredatabricks.net/api/2.0/jobs/$Mode" -Headers @{Authorization = $InternalBearerToken}
    }
    Catch {
        Write-Output "StatusCode:" $_.Exception.Response.StatusCode.value__ 
        Write-Output "StatusDescription:" $_.Exception.Response.StatusDescription
        Write-Error $_.ErrorDetails.Message
    }

    if ($Mode -eq "create") {
        Return $JobDetails.job_id
    }
    else {
        Return $JobId
    }
}

Add-DatabricksNotebookJob -BearerToken $BearerToken -Region $Region -JobName $JobName -SparkVersion $SparkVersion -NodeType $NodeType -MinNumberOfWorkers 1 -MaxNumberOfWorkers 2 -MaxRetries 1 -ScheduleCronExpression "0 15 22 ? * *" -Timezone "UTC" -NotebookPath $NotebookPath
