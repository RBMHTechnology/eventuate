#!/bin/bash -eu

export PW=<...>

CaAlias=rootca
CaKeystore=rootca.jks
CaExport=root.crt

CertAlias=serverandclient
CertKeystore=keystore.jks
CertSigningRequest=serverandclient.csr
CertSigned=serverandclient.crt

logStep() {
    echo
    echo '=========' "$@"
    echo
}

rm -f "$CaKeystore" "$CertKeystore" "$CaExport" "$CertSigningRequest" "$CertSigned"

logStep Create root CA in $CaKeystore
keytool -genkeypair \
  -alias "$CaAlias" \
  -dname "CN=CA" \
  -keystore "$CaKeystore" \
  -keypass:env PW \
  -storepass:env PW \
  -keyalg RSA \
  -keysize 4096 \
  -ext KeyUsage:critical="keyCertSign" \
  -ext BasicConstraints:critical="ca:true" \
  -validity 3700

logStep Export root CA to $CaExport for import into truststore
keytool -export \
  -alias "$CaAlias" \
  -file "$CaExport" \
  -keypass:env PW \
  -storepass:env PW \
  -keystore "$CaKeystore" \
  -rfc

logStep Make $CertKeystore trust CA as signer
keytool -import \
  -alias "$CaAlias" \
  -file "$CaExport" \
  -keystore "$CertKeystore" \
  -storepass:env PW << EOF
yes
EOF

logStep Create certificate for client and server auth in $CertKeystore
keytool -genkeypair \
  -alias "$CertAlias" \
  -dname "CN=Unknown" \
  -keystore "$CertKeystore" \
  -keypass:env PW \
  -storepass:env PW \
  -keyalg RSA \
  -keysize 2048 \
  -validity 385

logStep Create a certificate signing request: $CertSigningRequest
keytool -certreq \
  -alias "$CertAlias" \
  -keypass:env PW \
  -storepass:env PW \
  -keystore "$CertKeystore" \
  -file "$CertSigningRequest"

logStep Sign certificate with CA: $CertSigned
keytool -gencert \
  -alias "$CaAlias" \
  -keypass:env PW \
  -storepass:env PW \
  -keystore "$CaKeystore" \
  -infile "$CertSigningRequest" \
  -outfile "$CertSigned" \
  -ext KeyUsage:critical="digitalSignature,keyEncipherment" \
  -rfc

logStep Import signed certificate back into $CertKeystore
keytool -import \
  -alias "$CertAlias" \
  -file "$CertSigned" \
  -keystore "$CertKeystore" \
  -storepass:env PW

logStep List $CertKeystore
keytool -list \
  -keystore "$CertKeystore" \
  -storepass:env PW
