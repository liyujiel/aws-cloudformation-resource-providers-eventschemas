package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.auth.signer.internal.BaseAws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;

public class SchemasSigner extends BaseAws4Signer implements Signer {
    private final String signingName;

    public SchemasSigner(String signingName) {
        this.signingName = signingName;
    }

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
        Aws4SignerParams signingParams = extractSignerParams(Aws4SignerParams.builder(), executionAttributes)
                .signingName(signingName)
                .build();
        return sign(request, signingParams);
    }
}
