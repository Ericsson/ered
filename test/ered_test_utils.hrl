%% Expect to receive a message within timeout.
-define(MSG(Pattern, Timeout),
        (fun () ->
                 receive
                     Pattern = M -> M
                 after
                     Timeout -> error({timeout, ??Pattern, erlang:process_info(self(), messages)})
                 end
         end)()).

%% Expect to receive a message within a second.
-define(MSG(Pattern), ?MSG(Pattern, 1000)).

%% Check message queue for optional messages.
-define(OPTIONAL_MSG(Pattern),
        receive
            Pattern -> ok
        after
            0 -> ok
        end).
