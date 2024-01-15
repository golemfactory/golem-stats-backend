from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import UserProfile
from .utils import generate_nonce, verify_signature
from datetime import datetime, timedelta
import jwt
from django.conf import settings
from rest_framework_simplejwt.tokens import RefreshToken, TokenError
from .jwt import CustomToken
from django.contrib.auth.models import User

from django.shortcuts import get_object_or_404


@api_view(["GET"])
def find_user_by_wallet_address(request):
    wallet_address = request.GET.get("walletAddress")

    # Use get_object_or_404 instead of objects.get for handling non-existent cases
    user_profile = get_object_or_404(UserProfile, wallet_address=wallet_address)

    user = user_profile.user
    return Response(
        {
            "id": user.id,
            "walletAddress": user.userprofile.wallet_address,
            "web3Nonce": user.userprofile.web3_nonce,
        }
    )


@api_view(["POST"])
def create_user_on_backend(request):
    wallet_address = request.data.get("walletAddress")

    # First, check if a UserProfile with this wallet_address already exists
    try:
        profile = UserProfile.objects.get(wallet_address=wallet_address)
        user = profile.user
    except UserProfile.DoesNotExist:
        # If not, create a new User and UserProfile
        user = User.objects.create_user(
            username=wallet_address, password=None
        )  # Generate a unique username
        profile = UserProfile.objects.create(
            user=user, wallet_address=wallet_address, web3_nonce=generate_nonce()
        )

    return Response(
        {
            "id": user.id,
            "walletAddress": profile.wallet_address,
            "web3Nonce": profile.web3_nonce,
        }
    )


@api_view(["POST"])
def verify_wallet_signature(request):
    wallet_address = request.data.get("walletAddress")
    signature = request.data.get("web3NonceSignature")

    try:
        # Retrieve the UserProfile using wallet_address
        user_profile = UserProfile.objects.get(wallet_address=wallet_address)
        user = user_profile.user

        if verify_signature(user_profile.web3_nonce, signature, wallet_address):
            user_profile.update_nonce()

            refresh = CustomToken.for_user(user)

            return Response(
                {
                    "refreshToken": str(refresh),
                    "accessToken": str(refresh.access_token),
                    "id": user.id,
                    "walletAddress": user_profile.wallet_address,
                },
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                {"detail": "Invalid signature"}, status=status.HTTP_401_UNAUTHORIZED
            )

    except UserProfile.DoesNotExist:
        return Response({"detail": "User not found"}, status=status.HTTP_404_NOT_FOUND)


@api_view(["POST"])
def refresh_token(request):
    refresh_token = request.data.get("refreshToken")

    try:
        # Use simplejwt's RefreshToken to validate and create a new access token
        refresh = RefreshToken(refresh_token)
        new_access_token = str(refresh.access_token)

        return Response({"accessToken": new_access_token}, status=status.HTTP_200_OK)
    except TokenError as e:
        return Response({"detail": str(e)}, status=status.HTTP_401_UNAUTHORIZED)
