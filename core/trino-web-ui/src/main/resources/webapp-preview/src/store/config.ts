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
import { create } from "zustand";
import { persist } from "zustand/middleware";
import { StoreKey } from "../constant";

export enum Theme {
  Auto = "auto",
  Dark = "dark",
  Light = "light",
}

export const DEFAULT_CONFIG = {
  avatar: "",
  theme: Theme.Auto as Theme,
};

export type AppConfig = typeof DEFAULT_CONFIG;

export type AppConfigStore = AppConfig & {
  reset: () => void;
  update: (updater: (config: AppConfig) => void) => void;
};

export const useConfigStore = create<AppConfigStore>()(
    persist(
        (set, get) => ({
          ...DEFAULT_CONFIG,

          reset() {
            set(() => ({ ...DEFAULT_CONFIG }));
          },

          update(updater) {
            const config = { ...get() };
            updater(config);
            set(() => config);
          },
        }),
        {
          name: StoreKey.Config,
          version: 1,
          migrate(persistedState, version) {
            const state = persistedState as AppConfig;

            if (version < 1) {
              // merge your old config
            }
            /* eslint-disable  @typescript-eslint/no-explicit-any */
            return state as any;
          },
        },
    ),
);
