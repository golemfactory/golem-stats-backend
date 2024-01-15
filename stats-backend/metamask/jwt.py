from rest_framework_simplejwt.tokens import RefreshToken

class CustomToken(RefreshToken):
    @classmethod
    def for_user(cls, user):
        token = super().for_user(user)
        # Add custom claims
        token['wallet_address'] = user.wallet_address
        return token
