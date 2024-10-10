/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import React, { useEffect, useState, ReactNode } from 'react';
import { AuthContext, LogoutParams } from "./AuthContext";
import { authInfoApi, loginApi, logoutApi } from '../api/webapp/auth'
import { Texts } from '../constant';

export interface AuthInfo
{
  authType: "insecure" | "form" | "fixed" | "oauth2" | null;
  isPasswordAllowed: boolean;
  isAuthenticated: boolean;
  username?: string;
}
interface AuthProviderProps {
  children: ReactNode;  // This will allow any valid JSX (components, elements) as children
}

/* eslint-disable @typescript-eslint/no-explicit-any */
interface ApiCallOptions {
  apiFn: (params: any) => Promise<any>;
  onSuccess: (result: any) => void;
  params?: any;
  onForbidden?: (response: any) => void;
  onError?: (response: any) => void;
}
/* eslint-enable @typescript-eslint/no-explicit-any */

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [authInfo, setAuthInfo] = useState<AuthInfo | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const callApi = ({ apiFn, onSuccess, params, onForbidden, onError }: ApiCallOptions) => {
    setError(null);
    setLoading(true);
    apiFn(params).then((apiResponse) => {
      if (apiResponse.ok) {
        setError(null);
        onSuccess(apiResponse.result);
      } else if (apiResponse.status === 403) {
        onForbidden ? onForbidden(apiResponse) :
          setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.statusText}`);
      } else {
        onError ? onError(apiResponse) :
          setError(Texts.Auth.NotAvailableAuthInfo);
      }
      setLoading(false);
    })
  }

  useEffect(() => {
    if (!authInfo) {
      setError(null);
      setLoading(true);
      callApi({
        apiFn: authInfoApi,
        onSuccess: (result) => setAuthInfo(result),
      });
    }
  }, [authInfo])

  const login = async (username: string, password: string) => {
    if (authInfo) {
      callApi({
        apiFn: loginApi,
        onSuccess: () => {
          setAuthInfo({
            ...authInfo,
            isAuthenticated: true,
            username,
          })
        },
        params: {
          username: username, password: password
        },
        onForbidden: () => setError(Texts.Auth.InvalidUsernameOrPassword),
      });
    } else {
      setError(Texts.Auth.NotAvailableAuthInfo);
    }
  }

  const logout = ({ redirect = false }: LogoutParams) => {
    if (redirect) {
      window.location.href = "/ui/logout";
    } else {
      callApi({
        apiFn: logoutApi,
        onSuccess: () => {
          setError(null);
          setAuthInfo(null);
        },
      });
    }
  }

  return (
      <AuthContext.Provider value={{ authInfo, login, logout, loading, error }}>
        {children}
      </AuthContext.Provider>
  );
};
