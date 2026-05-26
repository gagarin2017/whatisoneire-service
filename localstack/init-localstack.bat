@echo off
setlocal
cls
echo =====================================================
echo === Provisioning LocalStack Infrastructure via CDK ===
echo =====================================================
echo.

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
set "CDK_DIR=%SCRIPT_DIR%infra-cdk"
set "JAR_PATH=%ROOT_DIR%\infra\target\scala-3.3.5\whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar"

where sbt >nul 2>nul
if errorlevel 1 (
    echo [ERROR] sbt is not installed or not on PATH.
    exit /b 1
)

where cdklocal >nul 2>nul
if errorlevel 1 (
    echo [ERROR] cdklocal is not installed or not on PATH.
    echo Please run: npm install -g aws-cdk-local aws-cdk
    exit /b 1
)

echo Building Scala Lambda fat jar...
pushd "%ROOT_DIR%"
call sbt "infra/assembly"
if errorlevel 1 (
    popd
    exit /b 1
)

if not exist "%JAR_PATH%" (
    echo [ERROR] Expected Lambda jar was not generated:
    echo %JAR_PATH%
    popd
    exit /b 1
)
popd

set AWS_ACCESS_KEY_ID=mock_key
set AWS_SECRET_ACCESS_KEY=mock_secret
set AWS_DEFAULT_REGION=eu-west-1
set AWS_REGION=eu-west-1
set CDK_DEFAULT_ACCOUNT=000000000000
set CDK_DEFAULT_REGION=eu-west-1

echo Navigating to CDK directory...
pushd "%CDK_DIR%"

echo Bootstrapping CDK resources in LocalStack...
call npm run bootstrap:local
if errorlevel 1 (
    popd
    exit /b 1
)

echo Deploying CloudFormation Stack to LocalStack...
call npm run deploy:local
if errorlevel 1 (
    popd
    exit /b 1
)
popd

echo.
echo =====================================================
echo === All Infrastructure Resources Deployed Successfully! ===
echo =====================================================
pause
