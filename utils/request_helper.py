"""
Request Helper
Utility to extract request information for audit logging and security monitoring
"""
from fastapi import Request
from typing import Optional


class RequestHelper:
    """Helper to extract request information for audit logging"""
    
    @staticmethod
    def get_client_ip(request: Optional[Request] = None) -> str:
        """
        Extract client IP from request.
        Handles proxies (X-Forwarded-For, X-Real-IP).
        
        Args:
            request: FastAPI Request object (optional, will try to get from context)
            
        Returns:
            str: Client IP address or "unknown"
        """
        if not request:
            try:
                from context_manager.context import get_request
                request = get_request()
            except:
                return "unknown"
        
        if not request:
            return "unknown"
        
        # Check for proxy headers (in order of preference)
        if forwarded := request.headers.get("X-Forwarded-For"):
            # X-Forwarded-For can contain multiple IPs, take the first one
            return forwarded.split(",")[0].strip()
        
        if real_ip := request.headers.get("X-Real-IP"):
            return real_ip
        
        # Direct connection
        if request.client:
            return request.client.host
        
        return "unknown"
    
    @staticmethod
    def get_user_agent(request: Optional[Request] = None) -> str:
        """
        Extract user agent from request.
        
        Args:
            request: FastAPI Request object (optional, will try to get from context)
            
        Returns:
            str: User agent string or "unknown"
        """
        if not request:
            try:
                from context_manager.context import get_request
                request = get_request()
            except:
                return "unknown"
        
        if not request:
            return "unknown"
        
        return request.headers.get("User-Agent", "unknown")
    
    @staticmethod
    def get_endpoint(request: Optional[Request] = None) -> str:
        """
        Get the endpoint path.
        
        Args:
            request: FastAPI Request object (optional, will try to get from context)
            
        Returns:
            str: Endpoint path (e.g., "/api/v1/login") or "unknown"
        """
        if not request:
            try:
                from context_manager.context import get_request
                request = get_request()
            except:
                return "unknown"
        
        if not request:
            return "unknown"
        
        return request.url.path
    
    @staticmethod
    def get_request_info(request: Optional[Request] = None) -> dict:
        """
        Get all request info for audit logging.
        
        Args:
            request: FastAPI Request object (optional, will try to get from context)
            
        Returns:
            dict: Dictionary with ip_address, user_agent, and endpoint
        """
        return {
            "ip_address": RequestHelper.get_client_ip(request),
            "user_agent": RequestHelper.get_user_agent(request),
            "endpoint": RequestHelper.get_endpoint(request)
        }
    
    @staticmethod
    def get_request_method(request: Optional[Request] = None) -> str:
        """
        Get the HTTP method (GET, POST, etc.).
        
        Args:
            request: FastAPI Request object (optional, will try to get from context)
            
        Returns:
            str: HTTP method or "unknown"
        """
        if not request:
            try:
                from context_manager.context import get_request
                request = get_request()
            except:
                return "unknown"
        
        if not request:
            return "unknown"
        
        return request.method

