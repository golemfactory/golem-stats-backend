from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import User
from .utils import generate_nonce, verify_signature
from datetime import datetime, timedelta
import jwt
from django.conf import settings
from rest_framework_simplejwt.tokens import RefreshToken, TokenError
from .jwt import CustomToken

@api_view(['GET'])
def find_user_by_wallet_address(request):
    wallet_address = request.GET.get('walletAddress')
    user = User.objects.get(wallet_address=wallet_address)
    return Response({'id': user.id, 'walletAddress': user.wallet_address, 'web3Nonce': user.web3_nonce})


@api_view(['POST'])
def create_user_on_backend(request):
    wallet_address = request.data.get('walletAddress')
    user, created = User.objects.get_or_create(wallet_address=wallet_address)
    if created:
        user.web3_nonce = generate_nonce() 
        user.save()
    return Response({'id': user.id, 'walletAddress': user.wallet_address, 'web3Nonce': user.web3_nonce})

@api_view(['POST'])
def verify_wallet_signature(request):
    wallet_address = request.data.get('walletAddress')
    signature = request.data.get('web3NonceSignature')

    try:
        user = User.objects.get(wallet_address=wallet_address)

        if verify_signature(user.web3_nonce, signature, wallet_address):
            user.update_nonce()

            refresh = CustomToken.for_user(user)

            return Response({
            'refreshToken': str(refresh),
            'accessToken': str(refresh.access_token),
            'id': user.id,
            'walletAddress': user.wallet_address
        }, status=status.HTTP_200_OK)
        else:
            return Response({'detail': 'Invalid signature'}, status=status.HTTP_401_UNAUTHORIZED)

    except User.DoesNotExist:
        return Response({'detail': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
    

@api_view(['POST'])
def refresh_token(request):
    refresh_token = request.data.get('refreshToken')

    try:
        # Use simplejwt's RefreshToken to validate and create a new access token
        refresh = RefreshToken(refresh_token)
        new_access_token = str(refresh.access_token)

        return Response({'accessToken': new_access_token}, status=status.HTTP_200_OK)
    except TokenError as e:
        return Response({"detail": str(e)}, status=status.HTTP_401_UNAUTHORIZED)
