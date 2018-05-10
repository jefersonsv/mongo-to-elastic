FROM microsoft/dotnet:2.0-sdk AS build-env

RUN apt-get update && apt-get install -y \
  nano

WORKDIR /app

# copy csproj and restore as distinct layers
COPY *.csproj ./
RUN dotnet restore

# copy everything else and build
COPY . ./
RUN dotnet publish -c Release -o out

# build runtime image
# microsoft/dotnet:2.0-runtime # don't work
FROM microsoft/dotnet:2.0-runtime
WORKDIR /app
COPY --from=build-env /app/out ./

ENTRYPOINT ["dotnet", "mongo-to-elastic.dll"]