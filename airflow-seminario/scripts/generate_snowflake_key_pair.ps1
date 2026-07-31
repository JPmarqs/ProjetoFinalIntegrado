[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

function Show-PublicKeyInstructions {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PrivateKeyPath,

        [Parameter(Mandatory = $true)]
        [string]$PublicKeyPath
    )

    $publicKeyBody = (
        Get-Content -LiteralPath $PublicKeyPath |
            Where-Object { $_ -notmatch '^-----' }
    ) -join ''

    Write-Host ''
    Write-Host 'Chaves disponiveis:'
    Write-Host "  Privada: $PrivateKeyPath"
    Write-Host "  Publica: $PublicKeyPath"
    Write-Host ''
    Write-Host 'Copie somente o conteudo abaixo para o marcador COLE_AQUI_A_CHAVE_PUBLICA no SQL:'
    Write-Host $publicKeyBody
    Write-Host ''
    Write-Warning 'Nunca envie ou faca commit do arquivo snowflake_rsa_key.p8.'
}

$keyDirectory = Join-Path $PSScriptRoot '..\keys'
$privateKey = Join-Path $keyDirectory 'snowflake_rsa_key.p8'
$publicKey = Join-Path $keyDirectory 'snowflake_rsa_key.pub'
$temporaryKey = Join-Path $keyDirectory 'snowflake_rsa_key_temp.pem'

New-Item -ItemType Directory -Force -Path $keyDirectory | Out-Null

$privateKeyExists = Test-Path -LiteralPath $privateKey
$publicKeyExists = Test-Path -LiteralPath $publicKey

if (-not $Force -and $privateKeyExists -and $publicKeyExists) {
    Write-Host 'As chaves ja existem; nenhuma chave foi substituida.'
    Show-PublicKeyInstructions -PrivateKeyPath $privateKey -PublicKeyPath $publicKey
    return
}

if (-not $Force -and ($privateKeyExists -or $publicKeyExists)) {
    throw 'O par de chaves esta incompleto. Preserve o arquivo existente e verifique a pasta keys antes de continuar.'
}

$opensslCommand = Get-Command openssl -ErrorAction SilentlyContinue
$opensslPath = if ($opensslCommand) {
    $opensslCommand.Source
}
else {
    @(
        'C:\Program Files\Git\usr\bin\openssl.exe',
        'C:\Program Files\Git\mingw64\bin\openssl.exe'
    ) |
        Where-Object { Test-Path -LiteralPath $_ } |
        Select-Object -First 1
}

if (-not $opensslPath) {
    throw 'OpenSSL nao foi encontrado. Instale o Git for Windows ou o OpenSSL e tente novamente.'
}

try {
    & $opensslPath genrsa -out $temporaryKey 2048
    if ($LASTEXITCODE -ne 0) {
        throw 'Falha ao gerar a chave RSA.'
    }

    & $opensslPath pkcs8 -topk8 -inform PEM -in $temporaryKey -out $privateKey -nocrypt
    if ($LASTEXITCODE -ne 0) {
        throw 'Falha ao converter a chave privada para PKCS#8.'
    }

    & $opensslPath rsa -in $privateKey -pubout -out $publicKey
    if ($LASTEXITCODE -ne 0) {
        throw 'Falha ao gerar a chave publica.'
    }
}
finally {
    if (Test-Path -LiteralPath $temporaryKey) {
        Remove-Item -LiteralPath $temporaryKey -Force
    }
}

Write-Host 'Chaves geradas com sucesso.'
Show-PublicKeyInstructions -PrivateKeyPath $privateKey -PublicKeyPath $publicKey
